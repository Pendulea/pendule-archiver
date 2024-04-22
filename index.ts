import { MyDB } from "./lib/db";

const main = async () => {
    const db = new MyDB('CTSIUSDT', '2020-04-23')
    await db.init()
}

main()