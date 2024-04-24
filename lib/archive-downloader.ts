import path from "path";
import { ARCHIVE_FOLDER } from "./constant";
import { MyDB } from "./models/db";
import { buildDateStr } from "./utils";
import downloadEngine, { DownloadEngine } from "./models/download-engine";
import processEngine from "./models/process-engine";
import fs from "fs";

export const downloadSymbolArchives = async (db: MyDB) => {
    let i = 1;
    const folderPath = path.join(ARCHIVE_FOLDER, db.symbol)
    !fs.existsSync(folderPath) && fs.mkdirSync(folderPath, { recursive: true })

    while (true){
        const date = buildDateStr(i)
        if (date < db.minHistoricalDate){
            break
        }
        const p = await db.isDateParsed(date)
        if (!p){
            const fileName = `${db.symbol}-trades-${date}.zip`;
            const fullPath = path.join(folderPath, fileName)
            downloadEngine.add(DownloadEngine.buildURL(db.symbol, date), fullPath, () => {
                processEngine.add(db, date)
            })
        }
        i++
    }
}
