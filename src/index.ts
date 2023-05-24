import * as readline from "node:readline/promises";
import { stdin as input, stdout as output } from "node:process";
import { send, close } from "./kafka";
const rl = readline.createInterface({ input, output });

async function run() {
  while (true) {
    const answer = await rl.question("");

    await send(answer);
  }
}

run().catch(console.error);

close().catch(console.error);
