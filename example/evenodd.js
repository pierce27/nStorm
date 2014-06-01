var Logger = require('arsenic-logger');
var nStorm = require('../index.js');
var even = 0
var odd = 0
var all = 0

/**
 * Demo spout that generates random data
 */
function RandSpout() {

    this.start = function(context) {

        sendData();

        function sendData(){
            
            var num = Math.floor(Math.random() * 101);

            var row = {number: num}

            // 'Emit' the data, which is passed to any blocks listening to
            // (subscribed) to this blocks messages
            context.emit(row);

            setTimeout(function(){
                sendData();
            }, 500);

        }

    }


}


function EvenOddBolt() {

    this.process = function(message, context) {

        // Logger.info("Processing ", message.number);


        if (message.number%2 == 0){
            var row = {name: 'even', number: message.number}
        } else {
            var row = {name: 'odd', number: message.number}
        }

        // Acknowledge
        context.ack(message);

        // Pass data along
        context.emit(row);

    }

}


function EvenBolt(){

    this.process = function(message, context){

        Logger.info(message.name)

        even = even + 1
        all = all+1

        if (even > 10){
            context.emit(message)
            even = 0
            odd = 0    
        }

        context.ack(message)

        
    }

}


function OddBolt(){

    this.process = function(message, context){

        Logger.info(message.name)
        odd=odd+1
        all = all+1

        if (odd > 10){
            context.emit(message)
            even = 0
            odd = 0    
        }

        context.ack(message)
    }
}

function WinBolt(){

    this.process = function(message, context){

        Logger.info("WINNER:", message.name)

        
        

        context.ack(message)
        context.emit(message)


    }
}





// Spout and Bolt implementation
var randSpout = new RandSpout();
var evenOddBolt = new EvenOddBolt();
var evenBolt = new EvenBolt();
var oddBolt = new OddBolt();
var winnerBolt = new WinBolt();

var cloud = new nStorm();

// Setting up topology using the topology builder
cloud.addBlock("randSpout", randSpout);
cloud.addBlock("evenOddBolt", evenOddBolt).input("randSpout");
cloud.addBlock("evenBolt", evenBolt).input("evenOddBolt", {name: "even"});
cloud.addBlock("oddBolt", oddBolt).input("evenOddBolt", {name: "odd"});
cloud.addBlock("winnerBolt", winnerBolt).input("evenBolt").input("oddBolt");

// Setup cluster, and run topology...
cloud.start();
