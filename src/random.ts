import { send } from "./kafka";
import { words } from "./words";
import { Message } from "./schema";

let timeout: NodeJS.Timeout | null = null;

const min = 1000;
const max = 5000;

export function sendRandom(message?: string) {
  // if (timeout) return;
  timeout = setTimeout(async () => {
    if (!message) {
      const random = Math.floor(Math.random() * words.length);
      message = words[random];
    }
    const randomNumber = Math.random() * (max - min + 1) + min;

    const content: Message = {
      message,
      country: "France",
      city: "Paris",
      temperature: 69,
      random: randomNumber,
      lat: 48.8534,
      long: 2.3488,
    };

    await send(content);

    console.log(`Sent ${message}`);
    timeout = null;
  }, 1000);
}
