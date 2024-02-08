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
    for (const key in row) {
        let value = row[key];
        if (value === '-9999') {
            value = null;
        } else if (key === 'station') {
            if (value === 'Indianapolis Airport') {
                value = '724380-93819';
            } // Add more station mappings as needed
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
    printDict(transformedRow);
}

//Helper function
function printDict(row) {
    for (let key in row){ 
        console.log(`${key}: ${row[key]}`);
    }
};