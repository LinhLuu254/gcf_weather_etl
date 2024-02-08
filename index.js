const {Storage} = require('@google-cloud/storage');
const {BigQuery} = require('@google-cloud/bigquery');

const weather_bq = new BigQuery();
const datasetId = 'weather_etl';
const tableId = 'weather_info';

const csv = require('csv-parser');

exports.readObservation = (file, context) => {
 
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
            if (value != null){
                value = parseInt(value);
            }else {
                value = null;
            }
           
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

    weatherInfo(transformedRow);
    
}

// Entry points
const weatherInfo = (row) => {
    const rowInfo = {
        station: row.station,
        year: row.year,
        month: row.month,
        hour: row.hour,
        airtemp: row.airtemp,
        dewpoint: row.dewpoint,
        pressure: row.pressure,
        winddirection: row.winddirection,
        windspeed: row.windspeed,
        sky: row.sky,
        precip1hour: row.precip1hour,
        precip6hour: row.precip6hour
    };

    writeToBq(rowInfo);
};

async function writeToBq(obj) {
    try {
        await weather_bq
            .dataset(datasetId)
            .table(tableId)
            .insert(obj);
        console.log('Insert:', obj);
    } catch (error) {
        console.error('Error in BigQuery insertion:', error);
    }
}