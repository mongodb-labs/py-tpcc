function checkConsistency(rule, collection, agg, sample=false) {
    agg.push({$count:"c"});
    if (sample) {
        var c=db.WAREHOUSE.count();
        var start=Math.round(Math.random()*c);
        var firstStage={$match:{}};
        var wid="W_ID";
        var did="D_ID";
        if (collection=="NEW_ORDER") {
          wid="NO_"+wid;
          did="NO_"+did;
        } else if (collection=="WAREHOUSE") {
          did=collection[0]+"_"+did;
        } else if (collection=="DISTRICT") {
          wid=collection[0]+"_"+wid;
        } else {
          wid=collection[0]+"_"+wid;
          did=collection[0]+"_"+did;
        }
        firstStage["$match"][wid]=start;
        /* firstStage["$match"][did]=(start%10)+1; */
        agg.unshift(firstStage);
    }
    var r = db.getCollection(collection).aggregate(agg, {maxTimeMS:60000}).toArray();
    if (r.length != 0) {
    print("Consistency rule " + rule + " failed with count " + r[0]["c"]);
    }
}

c=[];
c[1]={r:"1", c:"WAREHOUSE", pipeline: [
    {$lookup:{
        from:"DISTRICT",
        as:"d",
        localField:"W_ID",
        foreignField:"D_W_ID"}},
    {$match:{$expr:{$ne:[0,{$toLong:{$subtract:["$W_YTD",{$sum:"$d.D_YTD"}]}}]}}},
]};
// consistency check 2
c[2]={r:"2", c:"DISTRICT", pipeline: [
    {$project:{w:"$D_W_ID", d:"$D_ID",next:"$D_NEXT_O_ID"}},
    {$lookup:{from:"ORDERS",as:"maxOID",let:{w:"$w",d:"$d"},pipeline:[
        {$match:{$expr:{$and:[{$eq:["$$w","$O_W_ID"]},{$eq:["$$d","$O_D_ID"]}]}}},
        {$sort:{"O_ID":-1}},
        {$limit:1},
        {$group:{_id:0,maxO:{$first:"$O_ID"}}}]}},
    {$unwind:"$maxOID"},
    {$lookup:{from:"NEW_ORDER",as:"maxNOID",let:{w:"$w",d:"$d"},pipeline:[
        {$match:{$expr:{$and:[{$eq:["$$w","$NO_W_ID"]},{$eq:["$$d","$NO_D_ID"]}]}}},
        {$sort:{"NO_O_ID":-1}},
        {$limit:1},
        {$group:{_id:0,maxO:{$max:"$NO_O_ID"}}}]}},
    {$unwind:"$maxNOID"},
    {$match:{$or:[{$expr:{$ne:["$maxOID.maxO","$maxNOID.maxO"]}}, {$expr:{$ne:[ "$maxOID.maxO",{$subtract:["$next",1]}]}}  ]}},
]};
// consistency check 3
c[3]={r:"3", c:"NEW_ORDER", pipeline:[
    {$group:{_id:{w:"$NO_W_ID",d:"$NO_D_ID"},min:{$min:"$NO_O_ID"},max:{$max:"$NO_O_ID"},count:{$sum:1}}},
    {$project:{count:1, diff:{$add:[1,{$subtract:["$max","$min"]}]}}},
    {$match:{$expr:{$ne:["$count","$diff"]}}},
]};
// consistency check 4
c[4]={r:"4", c:"ORDERS", pipeline:[
    {$sort:{O_W_ID:1, O_D_ID:1}},
    {$limit:10000},
    {$group:{_id:{w:"$O_W_ID",d:"$O_D_ID"},O_CLs:{$sum:"$O_OL_CNT"}, OL_CLs:{$sum:{$size:"$ORDER_LINE"}}}},
    {$match:{$expr:{$ne:[ "$O_CLs","$OL_CLs"]}}},
]};
// consistency check 5
c[5]={r:"5", c:"ORDERS", pipeline:[
    {$match:{O_CARRIER_ID:0}},
    {$sort:{O_ID:-1}},
    {$limit:10000},
    {$lookup:{from:"NEW_ORDER", as:"NO_count",let:{w:"$O_W_ID",d:"$O_D_ID",o:"$O_ID"}, pipeline:[
        {$match:{$expr:{$and:[{$eq:["$$w","$NO_W_ID"]},{$eq:["$$d","$NO_D_ID"]},{$eq:["$$o","$NO_O_ID"]} ]}}},
        {$count:"c"}]}},
    {$addFields:{count:{$ifNull:[{$arrayElemAt:["$NO_count.c",0]},0]}}},
    {$match:{"count":{$ne:1}}},
]};
// consistency check 6
c[6]={r:"6", c:"ORDERS", pipeline:[
    {$limit:10000},
    {$match:{$expr:{$ne:[ "$O_OL_CNT",{$size:"$ORDER_LINE"}]}}},
]};
// consistency check 7
c[7]={r:"7", c:"ORDERS", pipeline:[
    {$limit:10000},
    {$match:{O_CARRIER_ID:{$ne:0}, "ORDER_LINE.OL_DELIVERY_D":null}},
]};
// consistency
c[8]={r:"8", c:"HISTORY", pipeline:[
    {$group:{_id:"$H_W_ID",sum:{$sum:{$toDecimal:"$H_AMOUNT"}}}},
    {$lookup:{from:"WAREHOUSE", localField:"_id", foreignField:"W_ID",as:"w"}},
    {$match:{$expr:{$ne:[0, {$toLong:{$subtract:["$sum", {$arrayElemAt:["$w.W_YTD",0]}]}}]}}},
]};
// consistency check 9
c[9]={r:"9", c:"HISTORY", pipeline:[
    {$group:{_id:{w:"$H_W_ID", d:"$H_D_ID"}, sum:{$sum:{$toDecimal:"$H_AMOUNT"}}}},
    {$lookup:{from:"DISTRICT", as:"d", let:{ w: "$_id.w", d:"$_id.d"}, pipeline:[
        {$match: {$expr: {$and: [ {$eq: ["$$w","$D_W_ID"]},{$eq:["$$d","$D_ID" ]}]}}},
        {$group:{_id:0, sum:{$sum:{$toDecimal:"$D_YTD"}}}}]}},
    {$match:{$expr:{$ne:[{$toLong:"$sum"},{$toLong:{$arrayElemAt:["$d.sum",0]}}]}}},
]};
// *** consistency check 10  don't run unless there is an index
/* adding one warehouse filter to limit checking, needed to add index to HISTORY.H_W_ID,etc to make reasonably fast even on one */
c[10]={r:"10", c:"CUSTOMER", pipeline:[
    {$match:{C_W_ID:1,C_D_ID:1,C_ID:{$lt:100,$gt:80}}},
    {$lookup:{from:"ORDERS", as:"o", let:{ w: "$C_W_ID", d:"$C_D_ID", c:"$C_ID"}, pipeline:[
        {$match: {O_CARRIER_ID:{$ne:0}, $expr: {$and: [ {$eq: ["$$w","$O_W_ID"]},{$eq:["$$d","$O_D_ID"]}, {$eq:["$$c","$O_C_ID"]}]}}},
        {$group:{_id:0, sum:{$sum:{$sum:"$ORDER_LINE.OL_AMOUNT"}}}}]}},
    {$lookup:{from:"HISTORY", as:"h", let:{ w: "$C_W_ID", d:"$C_D_ID", c:"$C_ID"}, pipeline:[
        {$match: {$expr: {$and: [ {$eq: ["$$w","$H_W_ID"]},{$eq:["$$d","$H_D_ID"]}, {$eq:["$$c","$H_C_ID"]}]}}},
        {$group:{_id:0, sum:{$sum:"$H_AMOUNT"}}}]}},
    {$project:{C_BALANCE:1, OSUM:{$ifNull:[{$arrayElemAt:["$o.sum",0]},0]},HSUM:{$arrayElemAt:["$h.sum",0]},_id:0, C_ID:1, C_W_ID:1, C_D_ID:1}},
    {$match:{$expr:{$ne:["$C_BALANCE", {$subtract:["$OSUM","$HSUM"]}]}}},
]};
// *** consistency check 11   Correct when first loaded!
c[11]={r:"11", c:"DISTRICT", pipeline:[
    {$project:{w:"$D_W_ID", d:"$D_ID"}},
    {$lookup:{from:"ORDERS",as:"o",let:{w:"$w",d:"$d"},pipeline:[
        {$match:{$expr:{$and:[{$eq:["$$w","$O_W_ID"]},{$eq:["$$d","$O_D_ID"]}]}}},
        {$count:"c"}]}},
    {$unwind:"$o"},
    {$lookup:{from:"NEW_ORDER",as:"no",let:{w:"$w",d:"$d"},pipeline:[
        {$match:{$expr:{$and:[{$eq:["$$w","$NO_W_ID"]},{$eq:["$$d","$NO_D_ID"]}]}}},
        {$count:"c"}]}},
    {$unwind:"$no"},
    {$match:{$expr:{$ne:[2100, {$subtract:["$o.c","$no.c"]}]}}},
]};
// consistency check 12
c[12]={r:"12", c:"CUSTOMER", pipeline:[
    {$lookup:{from:"ORDERS", as:"o", let:{ w: "$C_W_ID", d:"$C_D_ID", c:"$C_ID"}, pipeline:[
        {$match: {O_CARRIER_ID:{$ne:0},$expr: {$and: [ {$eq: ["$$w","$O_W_ID"]},{$eq:["$$d","$O_D_ID"]}, {$eq:["$$c","$O_C_ID"]}]}}},
        {$group:{_id:0, sum:{$sum:{$sum:"$ORDER_LINE.OL_AMOUNT"}}}}]}},
    {$project:{C_BALANCE:1, C_YTD_PAYMENT:1, OLSUM:{$ifNull:[{$arrayElemAt:["$o.sum",0]},0]}}},
    {$match:{$expr:{$ne:[0,{$toLong:{$subtract:["$OLSUM", {$add:["$C_BALANCE","$C_YTD_PAYMENT"]}]}}]}}},
]};

for (i=1; i<13; i++) {
  print (""+ new ISODate() + "  Checking " + i);
  if (i==10) continue;
  checkConsistency(c[i].r, c[i].c, c[i].pipeline,true);
}

