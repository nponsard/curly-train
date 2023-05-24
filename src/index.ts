import * as readline from "node:readline/promises";
import { stdin as input, stdout as output } from "node:process";
import { close, init } from "./kafka";
import { sendRandom } from "./random";
const rl = readline.createInterface({ input, output });

async function run() {
  await init();
  while (true) {
    const answer = await rl.question("");

    sendRandom(answer);
  }
}

run().catch(console.error);

close().catch(console.error);
