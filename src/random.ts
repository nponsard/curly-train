import { send } from './kafka';
import { words } from './words';

let timeout: NodeJS.Timeout | null = null;

export async function sendRandom() {
  // if (timeout) return;
  timeout = setTimeout(async () => {
    const random = Math.floor(Math.random() * words.length);
    const word = words[random];

    await send(word);

    console.log(`Sent ${word}`);
    timeout = null;
  }, 1000);
}