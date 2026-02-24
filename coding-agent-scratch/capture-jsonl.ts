/**
 * Captures JSONL output from the Claude Agent SDK subprocess
 * by making a vector completion request through the local server.
 *
 * Test 3: tool_call mode, "select test", responses: <image>(test.png), <image>(silly.jpg)
 */

import { ObjectiveAI, Vector } from "objectiveai";
import { spawn, ChildProcess } from "child_process";
import path from "path";
import fs from "fs";

delete process.env.CLAUDECODE;

function startServer(): Promise<ChildProcess> {
  return new Promise((resolve, reject) => {
    const ext = process.platform === "win32" ? ".exe" : "";
    const binary = path.resolve(
      __dirname,
      `../target/release/objectiveai-api${ext}`
    );
    const server = spawn(binary, [], {
      stdio: ["ignore", "pipe", "pipe"],
      env: { ...process.env },
    });

    let started = false;

    server.stderr!.on("data", (data: Buffer) => {
      const text = data.toString();
      process.stderr.write(text);
      if (!started && text.includes("listening")) {
        started = true;
        resolve(server);
      }
    });

    server.stdout!.on("data", (data: Buffer) => {
      const text = data.toString();
      process.stdout.write("[server] " + text);
      if (!started && text.includes("listening")) {
        started = true;
        resolve(server);
      }
    });

    server.on("error", reject);
    server.on("exit", (code) => {
      if (!started) reject(new Error(`Server exited with code ${code}`));
    });

    // Fallback: just wait a few seconds
    setTimeout(() => {
      if (!started) {
        started = true;
        resolve(server);
      }
    }, 5000);
  });
}

async function main() {
  console.log("Starting API server...");
  const server = await startServer();
  console.log("Server started, PID:", server.pid);

  try {
    const client = new ObjectiveAI({
      apiKey: "none",
      apiBase: "http://localhost:5000",
    });

    // Base64 encode the images
    const testPng = fs.readFileSync(path.resolve(__dirname, "test.png"));
    const sillyJpg = fs.readFileSync(path.resolve(__dirname, "silly.jpg"));
    const testB64 = `data:image/png;base64,${testPng.toString("base64")}`;
    const sillyB64 = `data:image/jpeg;base64,${sillyJpg.toString("base64")}`;

    console.log("Making vector completion request...");

    const result = await Vector.Completions.create(client, {
      messages: [
        { role: "user", content: "select test" },
      ],
      responses: [
        [{ type: "image_url", image_url: { url: testB64 } }],
        [{ type: "image_url", image_url: { url: sillyB64 } }],
      ],
      ensemble: {
        llms: [
          {
            model: "anthropic/claude-haiku-4.5",
            output_mode: "tool_call",
          },
        ],
      },
      profile: [1],
      upstreams: ["claude_agent_sdk"],
    });

    console.log("Result:", JSON.stringify(result, null, 2));

    // Save the unary response
    const jsonlFiles = fs.readdirSync(".").filter((f: string) => f.startsWith("claude_sdk_") && f.endsWith(".jsonl")).sort();
    const latestJsonl = jsonlFiles[jsonlFiles.length - 1];
    const timestamp = latestJsonl.replace("claude_sdk_", "").replace(".jsonl", "");
    fs.writeFileSync(`claude_sdk_${timestamp}_response.json`, JSON.stringify(result, null, 2));
    console.log(`\nSaved response to claude_sdk_${timestamp}_response.json`);
  } finally {
    console.log("Killing server...");
    server.kill("SIGTERM");
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
