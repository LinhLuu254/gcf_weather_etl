const {Storage} = require('@google-cloud/storage');

const csv = require('csv-parser');


exports.readObservation = (file, context) => {
    // console.log(`  Event: ${context.eventId}`);
    // console.log(`  Event Type: ${context.eventType}`);
    // console.log(`  Bucket: ${file.bucket}`);
    // console.log(`  File: ${file.name}`);

    const gcs = new Storage();

    const dataFile = gcs.bucket(file.bucket).file(file.name);

    dataFile.createReadStream()
    .on('error', () => {
        // Handle error
        console.error(error);
    })
    .pipe(csv())
    .on('data', (row) => {
        //Log row data
        // console.log(row);
        transformData(row);
    })
    .on('end', () => {
        //Handle end od CSV
        console.log('End');
    })
}
//Transform datd type and print out
function transformData(row) {
    const transformedRow = {};
    for (let key in row) {
        let value = row[key];
        if (value === '-999.9'|| value === '-9999'|| value === '-9999.0') {
            value = null;
        } else if (key === 'station') {
            value = '724380-93819'; 
        } else if (key === 'year' || key === 'month' || key === 'day' || key === 'hour' || key === 'winddirection' || key === 'sky') {
            value = parseInt(value);
        } else {
            const num = parseFloat(value);
            if (!isNaN(num)) {
                value = num / 10;
            } else {
                value = null;
            }
        }
        transformedRow[key] = value;
    }

    for (let key in transformedRow){ 
        console.log(`${key}: ${transformedRow[key]}`);
    }
    
    // printDict(transformedRow);
}

//Helper function
// function printDict(row) {
//     for (let key in row){ 
//         console.log(`${key}: ${row[key]}`);
//     }
// };