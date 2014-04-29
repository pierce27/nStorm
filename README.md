# nStorm

A simple real-time processing system inspired by [Storm](http://storm.incubator.apache.org/) and heavily inspired by [SimpleStorm](https://www.npmjs.org/package/simplestorm).

## Overview

The motivation for this work is to enable the easy creation of processing blocks that can be chained together in flexibile ways, coupled with the ability to scale by adding additional servers (workers).

The high-level objectives are;

1. Ability to create simple processing blocks that can be easily chained together (create topologies).
2. Easily scale by adding new servers/workers, ideally automatically.
3. Automatic replay of messages. If a process dies, then re-send message to another worker.

### Dependencies

To enable simple cross-server communications and replay of messages I made use of Redis, which is used for both a message store and also a pub-sub mechanism.

## Installation

```
npm install nstorm
```

## Usage

Include nStorm by 

```
var nStorm = require('nstorm');
```

Just as in Storm, you can create two types of blocks;

1. *Spouts* Spouts are data sources, and must contain a start function that `emits` data.
2. *bolts* Are chainable processing blocks, that must contain a process function.

### Creating a Spout

Spouts are fairly easy to create, the only restriction is that the Spout must contain a `start(context)` function. To output any messages, call the `emit(data)` function of the `context` object passed into the start function. 

The data you pass should be an Array or Object.

The following spout demonstrates a coin toss, it randomly 'emits' a head or a tails, and emits that as an object.

```
/**
 * Demo spout that generates random data
 */
function CoinSpout() {

    this.start = function(context) {

        sendData();

        function sendData(){
            
            var test = Math.floor(Math.random() * 101);

            var toss = 'tails';
            if (test < 50){
                toss = 'heads';
            }

            var row = {coin: toss, time: Date.now()}

            // 'Emit' the data, which is passed to any blocks listening to
            // (subscribed) to this blocks messages
            context.emit(row);

            setTimeout(function(){
                sendData();
            }, 1000);

        }

    }


}
```

### Creating a Bolt

Creating a bolt is also pretty straight forward. The only restriction is that they must contain a process function. This function is passed a message and the context oject. The message will be whatever data was emitted by a block that this block is connected to. So for example, if this was listening to the spout we just created then the message would be the result of the coin toss.

The `context` object contains two functions, an `emit()` which allows you to pass data to other blocks. And a `ack(message)`. This is important, the block must call ack on the message otherwise this message will be replayed (sent back to this block)! 

```
function HeadsBolt() {

    this.process = function(message, context) {

        Logger.info("Heads ", message.coin);


        // Acknowledge
        context.ack(message);

        // Pass data along
        context.emit(message);

    }

}
```

### Setting up a toplogy

Once you have created your blocks, now you can wire them up! 

Here is an example based on the bolt and spout we just created

```
var Logger = require('arsenic-logger');
var nStorm = require('nstorm');
var builder = new TopologyBuilder();

// Spout and Bolt implementation
var coinTossSpout = new CoinSpout();
var headsBolt = new HeadsBolt();

// Setting up topology using the topology builder
builder.setSpout("coinTossSpout", coinTossSpout);
builder.setBolt("headsBolt", headsBolt).input("coinTossSpout");

var cluster = new LocalCluster();
cluster.submitTopology("test", {}, builder.createTopology());
```

#### Parallelism

By default, every time a block gets a message its processing method is called. However, you can restrict the number of instances *per worker* by specifying a limit, e.g.

```
builder.setBolt("headsBolt", headsBolt, 2).input("coinTossSpout");
```

Here a maximum of 2 instances of the headsBolt will run. Any messages sent to that block will be buffered (using redis) until the block is ready.

Note, for this to work you must call `context.ack(message)` so nStorm knows you've finished processing the message.

#### Chaining inputs

Inputs can be chained, e.g.;

```
builder.setBolt("resultsBolt", resultsBolt, 1).input("tailsBolt").input("headsBolt");
```

So, here a 'resultsBot' is getting inputs from both a `tailsBolt` and a `headsBolt`.

#### Filtering

Inputs can be filted on a specific key and value, e.g

```
builder.setBolt("headsBolt", headsBolt, 1).input("coinTossSpout", {coin: "heads"});
```

Here the 'headsBolt' will reject any messages that do not have a "coin" key which equals "heads", i.e. it only accepts coin tosses of heads!

### Adding remote workers

Coming soon.

### Monitoring

Coming soon.

