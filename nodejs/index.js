// - get command line arguments
var argv = require('minimist')(process.argv.slice(2));
var redis_host = argv['redis_host']
var redis_port = argv['redis_port']
var subscribe_channel = argv['channel']

// - setup dependency instance
var express = require('express')
var app = express()
var server = require('http').createServer(app)
var io = require('socket.io')(server)

var redis = require('redis')
console.log('Creating redis client')
var redisclient = redis.createClient(redis_port, redis_host)
console.log('Subscribe to redis topic %s', subscribe_channel)
redisclient.subscribe(subscribe_channel)
redisclient.on('message', function (channel, message) {
    if (channel == subscribe_channel) {
        console.log('message received %s', message)
        io.sockets.emit('data', message)
    }
})

// - setup web app routing
app.use(express.static(__dirname + '/public'));
app.use('/jquery', express.static(__dirname + '/node_modules/jquery/dist/'));
app.use('/smoothie', express.static(__dirname + '/node_modules/smoothie/'));

app.get('/', function(req, res,next) {
    res.sendFile(__dirname + '/public/index.html');
});

server.listen(8080, function() {
    console.log('Server started at 8080');
});

// - setup shutdown hook
var shutdown_hook = function () {
    console.log('Quiting redis client')
    redisclient.quit()
    console.log('Shutting down app')
    process.exit();
}

process.on('SIGTERM', shutdown_hook)
process.on('SIGINT', shutdown_hook)
process.on('exit', shutdown_hook)
