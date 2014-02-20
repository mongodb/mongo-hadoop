NUM_DEVICES = 1000;
NUM_LOGS = NUM_DEVICES * 50 * 1000
setVerboseShell(false);

db.devices.remove()
db.logs.remove()

function getRandomInRange(from, to, fixed) {
    return (Math.random() * (to - from) + from).toFixed(fixed) * 1;
}

function getRandomString (len) {
    var possible = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    var randomString = '';
    for (var i = 0; i < len; i++) {
        var randomPoz = Math.floor(Math.random() * possible.length);
        randomString += possible.substring(randomPoz,randomPoz+1);
    }
    return randomString;
}

function randomDate(start, end) {
    return new Date(start.getTime() + Math.random() * (end.getTime() - start.getTime()))
}

function choose(choices) {
    index = Math.floor(Math.random() * choices.length);
    return choices[index];
}

function getRandomInt (min, max) {
  return Math.floor(Math.random() * (max - min) + min);
}

owners = []
for(var i=0;i<10;i++){
    owners.push(getRandomString(10));
}

models = []
for(var i=0;i<10;i++){
    models.push(getRandomInt(10, 20));
}

types = ["temp", "humidity", "pressure", "sound", "light"]


device_ids = []
// devices
//
for(var i=0;i<NUM_DEVICES;i++){

    var device_obj = {
        _id : ObjectId(),
        name : getRandomString(5) + getRandomInt(3),
        type : choose(types),
        owner : choose(owners),
        model : choose(models),
        created_at : randomDate(ISODate("2000-01-01T16:49:29.044-0400"), ISODate()),
    }

    device_ids.push(device_obj._id)
    db.devices.insert(device_obj)
}

for(var i=0;i<NUM_LOGS;i++){

    var log_obj = {
        _id : ObjectId(),
        d_id : choose(device_ids),
        v : Math.random() * getRandomInt(0, 10000),
        timestamp : randomDate(ISODate("2013-01-01T16:49:29.044-0400"), ISODate()),
        loc : [getRandomInRange(-180, 180, 3), getRandomInRange(-90, 90, 3)],
    }

    db.logs.insert(log_obj)
}






