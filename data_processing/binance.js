const config = require ('./config');
const MongoClient = require('mongodb').MongoClient;
const WebSocketClient = require('websocket').client;

async function processBinance() {
    const mongoClient = new MongoClient(config.mongo_uri, { useNewUrlParser: true, useUnifiedTopology: true });
    const webSocketClient = new WebSocketClient();

    await mongoClient.connect();
    const db = mongoClient.db('trades');

    // for each collection, implement TTL
    for (p of config.products) {
        await db.collection(p.toLowerCase()).createIndex( { "time": 1 }, { expireAfterSeconds: config.trade_ttl } )
    }

    let streams = [];
    for (m of config.markets.binance) {
        streams.push(m.toLowerCase() + '@aggTrade');
    }
    let getArgs = 'streams=';
    getArgs += streams.join('/');
    let binanceURI = 'wss://stream.binance.com:9443/stream?' + getArgs

    // catch failed connection
    webSocketClient.on('connectFailed', function(error) {
        console.log('Connect Error: ' + error.toString());
    });

    webSocketClient.on('connect', function(connection) {
        console.log('WebSocket Client Connected');
        // catch errors
        connection.on('error', function(error) {
            console.log("Connection Error: " + error.toString());
        });
        // catch close
        connection.on('close', function() {
            console.log('Connection Closed, retrying');
            // retry connection (connection automatically closed after 24h)
            webSocketClient.connect(binanceURI);
        });
        // catch message
        connection.on('message', function(message) {
            if (message.type === 'utf8') {
                data = JSON.parse(message.utf8Data);
                db.collection(data.stream).insertOne({
                    time: new Date(data.data.T),
                    data: {
                        p: data.data.p,
                        q: data.data.q,
                    }
                });
            }
        });
    });
    
    // connect to websocket
    webSocketClient.connect(binanceURI);
}

if (require.main === module) {
    processBinance();
}
