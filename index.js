// Imports

const config = require("./config.json")
const maxDifficulty = Buffer.from(config.maxThreshold,"hex");
const blake = require('blakejs');
const { WebSocketServer } = require('ws');

// Database Init

const { MongoClient } = require("mongodb");

const mongoclient = new MongoClient(config.MongoURI);

let mongostatus = false;
let db = null;
let blockCollection = null;
let serviceCollection = null;
let clientCollection = null;

mongoclient.connect().then(async()=>{
  console.log("Database connected!")
  db = mongoclient.db("main");
  blockCollection = db.collection("blocks");
  serviceCollection = db.collection("services");
  clientCollection = db.collection("clients");
  mongostatus = true;

});

async function getServicePWD(id) {
  const result = await serviceCollection.findOne({id});
  return result;
}

async function getPreCacheBlock(blockhash) {
  const result = await blockCollection.findOne({blockhash:blockhash.toString("binary")});
  if (result == null) return null;
  return Buffer.from(result.work,"binary");
}

function addPreCacheBlock(blockhash,work) {
  blockCollection.insertOne({blockhash:blockhash.toString("binary"),work:work.toString("binary")});
}

// Packet Encoding/Decoding

function getWorkThreshold(hash,work,defaultThreshold) {
  const hashBuffer = hash;
  const workBuffer = Buffer.from(work).reverse();
  if (hashBuffer.length != 32) throw Error("Hash has to be 32 bytes");
  if (workBuffer.length != 8) throw Error("Work has to be 8 bytes");
  const thresholdOutput = blake.blake2b(Buffer.concat([workBuffer,hashBuffer]),null,8);
  const threshold = Buffer.from(thresholdOutput).reverse()
  return (Buffer.compare(threshold,defaultThreshold) !== -1);
}

function decodeHashWork(packet,raw) {
  const hash = packet.subarray(0,32);
  const work = packet.subarray(32,40);
  return raw&&{hash,work}||{hash:hash.toString("hex"),work:work.toString("hex")}
}

function encodePacket(type,data) {
  const packet = Buffer.alloc(1+data.length);
  packet[0] = type;
  packet.set(data,1);
  return packet;
}

function decodePacket(packet) {
  const type = packet[0];
  const data = packet.subarray(1);
  return {type,data}
}

function decodeServiceKey(packet) {
  const userID = packet.subarray(0,32).toString("hex");
  const apiKey = packet.subarray(32,64).toString("hex");
  return {userID,apiKey};
}

// Socket

const nullByte = Buffer.from([]);

const workers = {};
const services = {};

var ondemandWork = {};

const workerSocketTypePAID = Buffer.from("0a","hex");
const workerSocketType = Buffer.from("0f","hex");
const serviceSocketType = Buffer.from("f0","hex");

const wss = new WebSocketServer({ port: 8080 });

let currentWID = 0;

const PacketTypes = {"0":"InitWorker","1":"InitService"}
const WorkerTypes = {"1":"Worker","2":"Service"}

const WorkerPacketTypes = {"2":"SubmitWork"}
const ServicePacketTypes = {"2":"RequestWork"}

function globalSendWorker(message) {
  Object.values(workers).forEach((worker)=>{
    worker.send(message)
  })
}

function incrementReward(publicKey) {
  clientCollection.updateOne({ account: publicKey }, { $inc: {ondemand: 1, pendingRewards: 10 }});
}

wss.on('connection', function connection(ws) {
  if (!mongostatus) return ws.close();
  const wID = ws.id = (currentWID++).toString()
  ws.socketType = null;
  ws.on("close",function(){
    delete workers[wID];
    delete services[wID];
  })
  ws.on('message', async function message(data) {
    const packetInfo = decodePacket(data);
    switch(PacketTypes[packetInfo.type]) {
      case "InitWorker": {
        if (ws.socketType !== null) return;
        const publicKey = packetInfo.data.toString("binary");
        if (publicKey.length == 32) {
          clientCollection.updateOne({ account: publicKey }, { $setOnInsert: { account: publicKey, ondemand: 0, pendingRewards: 0, paidRewards: 0 }}, { upsert: true });
          ws.send(encodePacket(0,workerSocketTypePAID));
          ws.publicKey = publicKey
        } else {
          ws.send(encodePacket(0,workerSocketType));
        }
        ws.socketType = 1;
        workers[wID] = ws;
        break;
      }
      case "InitService": {
        if (ws.socketType !== null) return;
        const serviceInfo = decodeServiceKey(packetInfo.data);
        const serviceQuery = await getServicePWD(serviceInfo.userID)
        if (!serviceQuery || serviceQuery.apiKey !== serviceInfo.apiKey) {
          ws.send(encodePacket(0,nullByte));
          return;
        }
        ws.socketType = 2;
        ws.serviceInfo = serviceInfo.userID;
        ws.send(encodePacket(0,serviceSocketType));
        services[wID] = ws;
        break;
      }
      default: {
        switch(WorkerTypes[ws.socketType]) {
          case "Worker": {
            switch(WorkerPacketTypes[packetInfo.type]) {
              case "SubmitWork": {
                const bufferHashWork = packetInfo.data.subarray(0,40);
                const { hash, work } = decodeHashWork(bufferHashWork,true);
                const blockHash = hash.toString("binary");
                const workDemand = ondemandWork[blockHash];
                if (!workDemand) return;
                const isValid = getWorkThreshold(hash, work, workDemand.difficulty);
                if (isValid) {
                  addPreCacheBlock(hash, work)
                  delete ondemandWork[blockHash];
                  const responsePacket = encodePacket(1,bufferHashWork);
                  const cancelPacket = encodePacket(2,hash);
                  globalSendWorker(cancelPacket);
                  workDemand.services.forEach((WID)=>{
                    if (!services[WID]) return;
                    services[WID].send(responsePacket);
                  });
                  if (ws.publicKey) {
                      incrementReward(ws.publicKey)
                  }
                }
                break;
              }
            }
            break;
          }
          case "Service": {
            switch(ServicePacketTypes[packetInfo.type]) {
              case "RequestWork": {
                if (packetInfo.data.length != 40) return;
                const bufferHashWork = packetInfo.data.subarray(0,40);
                const { hash, work: difficulty } = decodeHashWork(bufferHashWork,true);
                if (Buffer.compare(difficulty,maxDifficulty) == 1) return;
                const blockHash = hash.toString("binary");
                const cache = await getPreCacheBlock(hash);
                if (cache) {
                  ws.send(encodePacket(1,Buffer.concat([hash,cache])));
                  serviceCollection.updateOne({id:ws.serviceInfo},{$inc:{precache:1}})
                } else {
                  serviceCollection.updateOne({id:ws.serviceInfo},{$inc:{ondemand:1}})
                  if (ondemandWork[blockHash]) {
                    if (ondemandWork[blockHash].services.includes(wID)) return;
                    if (ondemandWork[blockHash].difficulty < difficulty) return;
                    ondemandWork[blockHash].services.push(wID)
                  } else {
                    ondemandWork[blockHash] = {services:[wID],difficulty};
                    globalSendWorker(encodePacket(1,bufferHashWork));
                  }
                }
                break;
              }
            }
            break;
          }
        }
      }
    }
  });
});