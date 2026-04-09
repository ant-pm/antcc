use anyhow::Result;
use aws_credential_types::Credentials;
use aws_sdk_s3::{Client, Config, config::Region, primitives::ByteStream};
use colonyos::core::{Executor, Software};
use serde::Deserialize;
use std::process::Command;
use tempfile::tempdir;
use tokio::signal;

#[derive(Debug, Deserialize)]
struct CompileJob {
    input_key: String,
    output_key: String,
    flags: Vec<String>,
}

fn minio_client() -> Client {
    let endpoint = std::env::var("MINIO_ENDPOINT").expect("MINIO_ENDPOINT not set");
    let access_key = std::env::var("MINIO_ACCESS_KEY").expect("MINIO_ACCESS_KEY not set");
    let secret_key = std::env::var("MINIO_SECRET_KEY").expect("MINIO_SECRET_KEY not set");

    let creds = Credentials::new(access_key, secret_key, None, None, "static");
    let config = Config::builder()
        .endpoint_url(endpoint)
        .credentials_provider(creds)
        .region(Region::new("us-east-1"))
        .force_path_style(true)
        .build();
    Client::from_conf(config)
}

pub async fn run_executor() -> Result<()> {
    let colony_name = std::env::var("COLONY").expect("COLONY not set");
    let prvkey = std::env::var("COLONY_PRIVATE_KEY").expect("COLONY_PRIVATE_KEY not set");
    let exec_prvkey = colonyos::crypto::gen_prvkey();
    let executor_id = colonyos::crypto::gen_id(&exec_prvkey);
    let executor_name = format!("antcc-executor-{}", hostname::get()?.to_string_lossy());

    let mut executor = Executor::new(&executor_name, &executor_id, "antcc-executor", &colony_name);
    executor.capabilities.software.push(Software {
        name: "gcc".into(),
        software_type: "compiler".into(),
        version: String::new(),
    });

    colonyos::add_executor(&executor, &prvkey).await?;
    colonyos::approve_executor(&colony_name, &executor_name, &prvkey).await?;
    println!("Executor registered: {executor_name}");

    let result = run_loop(&colony_name, &exec_prvkey).await;

    colonyos::remove_executor(&colony_name, &executor_name, &prvkey).await?;
    result
}

async fn run_loop(colony_name: &str, exec_prvkey: &str) -> Result<()> {
    let s3 = minio_client();
    let bucket = std::env::var("MINIO_BUCKET").unwrap_or_else(|_| "antcc".into());
    loop {
        tokio::select! {
            _ = signal::ctrl_c() => {
                println!("Shutting down...");
                return Ok(());
            }
            result = colonyos::assign(colony_name, 10, exec_prvkey) => {
                match result {
                    Err(e) if !e.conn_err() => continue,
                    Err(e) => {
                        eprintln!("Connection error: {e}");
                        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                    }
                    Ok(process) => {
                        println!("Assigned: {}", process.processid);

                        let res = handle_job(&s3, &bucket, &serde_json::to_value(&process.spec.kwargs).unwrap()).await;
                        match res {
                            Ok(output_key) => {
                                let out = vec![output_key];
                                if let Err(e) = colonyos::set_output(&process.processid, out, exec_prvkey).await {
                                    eprintln!("set_output failed: {e}");
                                } else if let Err(e) = colonyos::close(&process.processid, exec_prvkey).await {
                                    eprintln!("close failed: {e}");
                                } else {
                                    println!("Process {} done", process.processid);
                                }
                            }
                            Err(e) => {
                                eprintln!("Job failed: {e:#}");
                                let _ = colonyos::fail(&process.processid, exec_prvkey).await;
                            }
                        }
                    }
                }
            }
        }
    }
}

async fn handle_job(s3: &Client, bucket: &str, kwargs: &serde_json::Value) -> Result<String> {
    let job: CompileJob = serde_json::from_value(kwargs.clone())?;

    let tmp = tempdir()?;
    let i_path = tmp.path().join("input.i");
    let o_path = tmp.path().join("output.o");

    // Download .i
    let resp = s3
        .get_object()
        .bucket(bucket)
        .key(&job.input_key)
        .send()
        .await?;
    let i_bytes = resp.body.collect().await?.into_bytes();
    std::fs::write(&i_path, i_bytes)?;

    // Compile
    let status = Command::new("gcc")
        .arg("-c")
        .arg(&i_path)
        .arg("-o")
        .arg(&o_path)
        .args(&job.flags)
        .status()?;

    if !status.success() {
        return Err(anyhow::anyhow!("gcc exited {}", status));
    }

    // Upload .o
    let o_bytes = std::fs::read(&o_path)?;
    s3.put_object()
        .bucket(bucket)
        .key(&job.output_key)
        .body(ByteStream::from(o_bytes))
        .send()
        .await?;

    Ok(job.output_key)
}
