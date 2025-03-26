import AWS from 'aws-sdk';
import { parse } from 'fast-csv';
import dotenv from 'dotenv';
dotenv.config();

console.log(process.env.AWS_ACCESS_KEY_ID)

const s3Clinet = new AWS.S3({
    accessKeyId: 'key',
    secretAccessKey: 's',
    region: "us-east-1"
})

const importCSV = async(bucket,key) =>{

    const s3DataStream = s3Clinet.getObject({Bucket:bucket,Key:key}).createReadStream(); // this createReadstream is used for reading s3 data onot local data.
    const csvStream = parse({ headers: true });

    let batch = [];
    const BATCH_SIZE = 100;
    s3DataStream.pipe(csvStream)
    .on("data", (row)=>{
        batch.push([row.id, row.name, row.email]);

        console.log(batch);
        if (batch.length >= BATCH_SIZE) {
            // insertIntoDB(batch);
            batch = []; // Reset batch
          }
    }).on("end", async() =>{
        if(batch>0) 
            //await insertIntoDB(batch);
        console.log("CSV import complete!");
        //connection.end();
    }).on('error',()=>{
        console.error("Error processing CSV:", err);
    })
}



importCSV("data-lake-s3-source","raw/users_large.csv")


