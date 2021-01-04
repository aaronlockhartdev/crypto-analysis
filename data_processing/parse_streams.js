const config = require ('../config');
const MongoClient = require('mongodb').MongoClient;
const WebSocketClient = require('websocket').client;

async function initDB() {
    // for each collection, implement TTL
    for (p of config.products) {
        await db.collection(p.toLowerCase()).createIndex( { "trade_date": 1 }, { expireAfterSeconds: config.trade_ttl } )
    }
}

async function parseStream(uri, sub, proc) {
    const mongoClient = new MongoClient(config.mongo_uri, { useNewUrlParser: true, useUnifiedTopology: true });
    const webSocketClient = new WebSocketClient();

    await mongoClient.connect();
    const db = mongoClient.db('trades');

    // catch failed connection
    webSocketClient.on('connectFailed', function(error) {
        console.log('Connect Error: ' + error.toString());
    });

    webSocketClient.on('connect', function(connection) {
        console.log('WebSocket Client Connected');
        // send subscribe message
        if (connection.connected) {
            connection.sendUTF(JSON.stringify(sub));
        }
        // catch errors
        connection.on('error', function(error) {
            console.log("Connection Error: " + error.toString());
        });
        // catch close
        connection.on('close', function() {
            console.log('Connection Closed');
        });
        // catch message
        connection.on('message', function(message) {
            if (message.type === 'utf8') {
                data = JSON.parse(message.utf8Data);
                parsed = proc(data);
                if (parsed !== null) {
                    db.collection(data.product_id.toLowerCase()).insertOne(parsed, (err) => {
                        // catch errors
                        if (err !== null) console.err(err);
                    });
                }
            }
        });
    });
    // connect to websocket
    webSocketClient.connect(uri);
}

(() => {
    function procCoinbase(data) {
        if (data.type !== 'ticker') return null;

        // temp vars for computational efficiency
        let h = parseFloat(data.high_24h);
        let l = parseFloat(data.low_24h);
        let p = (parseFloat(data.price) - l) / (h - l);
        
        return {
            sequence: parseInt(data.sequence),
            price: p,
            volume: parseFloat(data.volume_24h) * 30 / parseFloat(data.volume_30d),
            best_bid: (parseFloat(data.best_bid) - l) / (h - l),
            best_ask: (parseFloat(data.best_ask) - l) / (h - l),
            side: data.side === 'buy' ? 1.0 : 0.0,
            last_size: data.last_size * p,
            trade_time: new Date(data.time)
        }
    }

    let subCoinbase = {
        "type": "subscribe",
        "product_ids": config.products,
        "channels": [
            "ticker"
        ]
    }

    function procBinance(data) {
        if (data.type !== 'ticker') return null;

        print(data)
        return {}
    }

    let subBinance = {
        "method": "SUBSCRIBE",
        "params": [
            "btcusdt@aggTrade",
            "btcusdt@depth"
        ],
        "id": 1
    }

    initDB();
    parseStream('wss://ws-feed.pro.coinbase.com', subCoinbase, procCoinbase);
    
    
})();
