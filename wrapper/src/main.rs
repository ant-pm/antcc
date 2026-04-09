use aws_credential_types::Credentials;
use aws_sdk_s3::{Client, Config, config::Region, primitives::ByteStream};
use std::process::{Command, exit};
use tempfile::tempdir;
use uuid::Uuid;

fn minio_client() -> Client {
    let creds = Credentials::new("antcc", "antccpass", None, None, "static");
    let config = Config::builder()
        .endpoint_url("http://localhost:9000")
        .credentials_provider(creds)
        .region(Region::new("us-east-1"))
        .force_path_style(true)
        .build();
    Client::from_conf(config)
}

fn split_args(args: &[String]) -> (Vec<String>, Vec<String>, String) {
    let output = args
        .windows(2)
        .find(|w| w[0] == "-o")
        .map(|w| w[1].clone())
        .unwrap_or_else(|| "a.o".into());

    let mut pp_args: Vec<String> = Vec::new();
    let mut remote_args: Vec<String> = Vec::new();
    let mut i = 0;

    while i < args.len() {
        match args[i].as_str() {
            "-c" => {
                pp_args.push("-E".into());
                // -c added separately for remote
            }
            "-o" => {
                i += 1; // skip -o + argument
            }
            "-MT" | "-MF" => {
                // keep for preprocessing, strip for remote
                pp_args.push(args[i].clone());
                if i + 1 < args.len() {
                    i += 1;
                    pp_args.push(args[i].clone());
                }
            }
            "-MD" | "-MMD" | "-MP" => {
                pp_args.push(args[i].clone()); // keep for preprocessing only
            }
            a if a == output => {}
            _ => {
                pp_args.push(args[i].clone());
                // Don't send source files to remote — we send input.i explicitly
                if !args[i].ends_with(".c") && !args[i].ends_with(".i") {
                    remote_args.push(args[i].clone());
                }
            }
        }
        i += 1;
    }

    (pp_args, remote_args, output)
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();

    if !args.contains(&"-c".to_string()) {
        let status = Command::new("gcc").args(&args).status().unwrap();
        exit(status.code().unwrap_or(1));
    }

    let (pp_args, remote_args, output) = split_args(&args);

    let tmp = tempdir().unwrap();
    let i_path = tmp.path().join("input.i");

    let t0 = std::time::Instant::now();

    let status = Command::new("gcc")
        .args(&pp_args)
        .arg("-o")
        .arg(&i_path)
        .status()
        .unwrap();

    if !status.success() {
        exit(1);
    }

    let preprocess_ms = t0.elapsed().as_millis();

    // ── MinIO upload ──────────────────────────────────────────────────────
    let job_id = Uuid::new_v4();
    let input_key = format!("jobs/{job_id}/input.i");
    let output_key = format!("jobs/{job_id}/output.o");

    let client = minio_client();
    let i_bytes = std::fs::read(&i_path).unwrap();
    let i_size = i_bytes.len();

    let t1 = std::time::Instant::now();

    client
        .put_object()
        .bucket("antcc")
        .key(&input_key)
        .body(ByteStream::from(i_bytes))
        .send()
        .await
        .unwrap();

    let upload_ms = t1.elapsed().as_millis();

    // ── Submit to ColonyOS ────────────────────────────────────────────────
    let colony_name = "dev";
    let prvkey = "ba949fa134981372d6da62b6a56f336ab4d843b22c02a4257dcf7d0d73097514";
    let colony_host = "colony.colonypm.xyz";
    colonyos::set_server_url(&format!("https://{colony_host}/api"));

    let mut fn_spec = colonyos::core::FunctionSpec::new("compile", "antcc-executor", &colony_name);
    fn_spec.kwargs.insert(
        "input_key".into(),
        serde_json::Value::String(input_key.clone()),
    );
    fn_spec.kwargs.insert(
        "output_key".into(),
        serde_json::Value::String(output_key.clone()),
    );
    fn_spec.kwargs.insert(
        "flags".into(),
        serde_json::Value::Array(
            remote_args
                .iter()
                .map(|f| serde_json::Value::String(f.clone()))
                .collect(),
        ),
    );
    fn_spec.maxexectime = -1;

    let proc = colonyos::submit(&fn_spec, &prvkey).await.unwrap();
    colonyos::subscribe_process(&proc, colonyos::core::SUCCESS, 120, &prvkey)
        .await
        .unwrap();
    // ─────────────────────────────────────────────────────────────────────

    // ── MinIO download ────────────────────────────────────────────────────
    let t2 = std::time::Instant::now();

    let resp = client
        .get_object()
        .bucket("antcc")
        .key(&output_key)
        .send()
        .await
        .unwrap();

    let o_bytes = resp.body.collect().await.unwrap().into_bytes();
    let download_ms = t2.elapsed().as_millis();

    std::fs::write(&output, o_bytes).unwrap();

    // Cleanup
    let _ = client
        .delete_object()
        .bucket("antcc")
        .key(&input_key)
        .send()
        .await;
    let _ = client
        .delete_object()
        .bucket("antcc")
        .key(&output_key)
        .send()
        .await;

    eprintln!(
        "[antcc] pre={preprocess_ms}ms upload={upload_ms}ms download={download_ms}ms size={i_size}B {output}"
    );
}
