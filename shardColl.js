// Run this before loading the data
sh.setBalancerState(false);
db.getSiblingDB("_DBNAME_").dropDatabase();
sleep(10000);
sh.enableSharding("_DBNAME_");
sh.shardCollection("_DBNAME_.ITEM", { "I_W_ID": 1, "I_ID": 1 }, true);
db.getSiblingDB("_DBNAME_").WAREHOUSE.createIndex({ "W_ID": 1, "W_TAX": 1 }, { unique: true });
sh.shardCollection("_DBNAME_.WAREHOUSE", { "W_ID": 1 });
db.getSiblingDB("_DBNAME_").DISTRICT.createIndex({ "D_W_ID": 1, "D_ID": 1, "D_NEXT_O_ID": 1, "D_TAX": 1 }, { unique: true });
sh.shardCollection("_DBNAME_.DISTRICT", { "D_W_ID": 1, "D_ID": 1 });
sh.shardCollection("_DBNAME_.CUSTOMER", { "C_W_ID": 1, "C_D_ID": 1, "C_ID": 1 }, true);
sh.shardCollection("_DBNAME_.HISTORY", { "H_W_ID": 1 });
sh.shardCollection("_DBNAME_.STOCK", { "S_W_ID": 1, "S_I_ID": 1 }, true);
db.getSiblingDB("_DBNAME_").NEW_ORDER.createIndex({ "NO_W_ID": 1, "NO_D_ID": 1, "NO_O_ID": 1 }, { unique: true });
sh.shardCollection("_DBNAME_.NEW_ORDER", { "NO_W_ID": 1, "NO_D_ID": 1 });
db.getSiblingDB("_DBNAME_").ORDERS.createIndex({ "O_W_ID": 1, "O_D_ID": 1, "O_ID": 1, "O_C_ID": 1 }, { unique: true });
sh.shardCollection("_DBNAME_.ORDERS", { "O_W_ID": 1, "O_D_ID": 1, "O_ID": 1 });
// this is for 6 WH on 3 shards
// make sure that number of WH is a multiple of number of shards
// nWH/nshards=whole number (2 here)
var numShards = _SHARDS_;
if (numShards <= 0) {
    print("Warning: Shards argument is negative. Getting shards from the cluster");
    numShards = db.getSiblingDB("config").shards.count();
}

var numWH = _NUMWAREHOUSES_;   /* must be multiple of 3 */
if (numWH <= 0) {
    print("Error: Invalid number of warehouses. numWH must be > 0");
    quit(64);
}

var remainder = numWH % numShards;
if (remainder !== 0) {
    print("ERROR: Number of Warehouses (" + numWH + ") is not a multiple of the number of shards (" + numShards + ")");
    quit(64);
}

var whPerShard = numWH / numShards;
print("Using  (" + numWH + ") warehouses per shard");

// do splits
for (i = 1 + whPerShard; i < numWH; i = i + whPerShard) {
    print("Splitting at " + i);
    sh.splitAt("_DBNAME_.ITEM", { "I_W_ID": i, "I_ID": MinKey });
    sh.splitAt("_DBNAME_.WAREHOUSE", { "W_ID": i });
    sh.splitAt("_DBNAME_.HISTORY", { "H_W_ID": i });
    sh.splitAt("_DBNAME_.DISTRICT", { "D_W_ID": i, "D_ID": MinKey });
    sh.splitAt("_DBNAME_.CUSTOMER", { "C_W_ID": i, "C_D_ID": MinKey, "C_ID": MinKey });
    sh.splitAt("_DBNAME_.STOCK", { "S_W_ID": i, "S_I_ID": MinKey });
    sh.splitAt("_DBNAME_.NEW_ORDER", { "NO_W_ID": i, "NO_D_ID": MinKey });
    sh.splitAt("_DBNAME_.ORDERS", { "O_W_ID": i, "O_D_ID": MinKey, "O_ID": MinKey });
}

// do moves
var shards = db.getSiblingDB("config").shards.distinct("_id");
for (i = 0; i < numShards; i = i + 1) {
    key = (i * whPerShard + 1);
    shd = shards[i];
    print("Moving " + key + " to shard " + shd);
    sh.moveChunk("_DBNAME_.ITEM", { "I_W_ID": key, "I_ID": MinKey }, shd);
    sh.moveChunk("_DBNAME_.WAREHOUSE", { "W_ID": key }, shd);
    sh.moveChunk("_DBNAME_.HISTORY", { "H_W_ID": key }, shd);
    sh.moveChunk("_DBNAME_.DISTRICT", { "D_W_ID": key, "D_ID": MinKey }, shd);
    sh.moveChunk("_DBNAME_.CUSTOMER", { "C_W_ID": key, "C_D_ID": MinKey, "C_ID": MinKey }, shd);
    sh.moveChunk("_DBNAME_.STOCK", { "S_W_ID": key, "S_I_ID": MinKey }, shd);
    sh.moveChunk("_DBNAME_.NEW_ORDER", { "NO_W_ID": key, "NO_D_ID": MinKey }, shd);
    sh.moveChunk("_DBNAME_.ORDERS", { "O_W_ID": key, "O_D_ID": MinKey, "O_ID": MinKey }, shd);
}

print("Shard configuration succeded");

