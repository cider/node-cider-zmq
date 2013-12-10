/*
  Copyright (c) 2013 Ondřej Kupka

  Permission is hereby granted, free of charge, to any person obtaining a copy
  of this software and associated documentation files (the "Software"),
  to deal in the Software without restriction, including without limitation
  the rights to use, copy, modify, merge, publish, distribute, sublicense,
  and/or sell copies of the Software, and to permit persons to whom
  the Software is furnished to do so, subject to the following conditions:

  The above copyright notice and this permission notice shall be included
  in all copies or substantial portions of the Software.

  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
  THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
  FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
  IN THE SOFTWARE.
*/

//------------------------------------------------------------------------------
// class Socket
//------------------------------------------------------------------------------

var Socket = {};

/*
 * Socket constructor function
 */

function createSocket() {
  return Object.create(Socket);
};

/*
 * Socket.connect
 */

Socket.connect = function connect(opts, next) {
  // No options were passed to connect.
  if (next === undefined) {
    next = opts;
    opts = {};
  }

  // Save next for later to be called after OHAI is received.
  this._next = next;

  // Make sure that all necessary arguments are set.
  var identity = opts.identity || process.env['CIDER_IDENTITY']

    , rpcEndpoint = opts.rpcEndpoint || process.env['CIDER_ZMQ_RPC_ENDPOINT']
    , rpcSndhwm   = opts.rpcSndhwm   || process.env['CIDER_ZMQ_RPC_SNDHWM']
    , rpcRcvhwm   = opts.rpcRcvhwm   || process.env['CIDER_ZMQ_RPC_RCVHWM']

    , pubEndpoint = opts.pubEndpoint || process.env['CIDER_ZMQ_PUB_ENDPOINT']
    , subRcvhwm   = opts.subRcvhwm   || process.env['CIDER_ZMQ_SUB_RCVHWM'];

  if (identity === undefined) {
    next(new Error("argument 'identity' is not set"));
    return;
  }

  if (rpcEndpoint === undefined) {
    next(new Error("argument 'rpcEndpoint' is not set"));
    return;
  }

  if (pubEndpoint === undefined) {
    next(new Error("argument 'pubEndpoint' is not set"));
    return;
  }

  // Set up the internal ZeroMQ objects.
  var rpcSock, subSock;
  try {
    rpcSock = zmq.socket('dealer');
    subSock = zmq.socket('sub');

    rpcSock.setsockopt(zmq.ZMQ_IDENTITY, new Buffer(identity));

    if (rpcSndhwm !== undefined) {
      rpcSndhwm = parseInt(rpcSndhwm);
      rpcSock.setsockopt(zmq.ZMQ_SNDHWM, rpcSndhwm);
    }
    if (rpcRcvhwm !== undefined) {
      rpcRcvhwm = parseInt(rpcRcvhwm);
      rpcSock.setsockopt(zmq.ZMQ_RCVHWM, rpcRcvhwm);
    }
    if (subRcvhwm !== undefined) {
      subRcvhwm = parseInt(subRcvhwm);
      subSock.setsockopt(zmq.ZMQ_RCVHWM, subRcvhwm);
    }

    rpcSock.on('message', this._handleRPCMessage);
    rpcSock.on('error',   this._handleRPCError);

    subSock.on('message', this._handlePUBMessage);
    subSock.on('error',   this._handlePUBError);

    // Connect to the broker.
    rpcSock.connect(rpcEndpoint);
    subSock.connect(subEndpoint);

    // Send the initial HELLO message.
    this._rpcSock = rpcSock;
    this._subSock = subSock;
    this._sendHello();
  } catch (err) {
    rpcSock.close();
    subSock.close();
    next(err);
  }
};

/*
 * Socket._sendHello
 */

Socket._sendHello = function _sendHello() {
  this._rpcSock.send([
      FRAME_EMPTY,
      FRAME_HEADER,
      FRAME_MSGTYPE_HELLO
  ]);
};

/*
 * Socket._sendKthxbye
 */

Socket._sendKthxbye = function _sendKthxbye() {
  this._send([
      FRAME_EMPTY,
      FRAME_HEADER,
      FRAME_MSGTYPE_KTHXBYE
  ]);
};

/*
 * Socket._sendRegister
 */

Socket._sendRegister = function _sendRegister(method) {
  this._send([
      FRAME_EMPTY,
      FRAME_HEADER,
      FRAME_MSGTYPE_REGISTER,
      method,
  ]);
};

/*
 * Socket._sendUnregister
 */

Socket._sendUnregister = function _sendUnregister(method) {
  this._send([
      FRAME_EMPTY,
      FRAME_HEADER,
      FRAME_MSGTYPE_UNREGISTER,
      method,
  ]);
};

/*
 * Socket.sendRequest
 */

Socket._sendRequest = function _sendRequest(req) {
  if (req._id === undefined) throw new Error('Request ID not defined');

  var reqId = new Buffer(2);
  reqId.writeUInt16BE(req._id, 0);

  var msgLength = 6;
  if (req.stdout.listeners('frame').length !== 0) msgLength += 2;
  if (req.stderr.listeners('frame').length !== 0) msgLength += 2;

  var msg = new Array(msgLength);
  msg[0] = FRAME_EMPTY;
  msg[1] = FRAME_HEADER;
  msg[2] = FRAME_MSGTYPE_REQUEST;
  msg[3] = reqId;
  msg[4] = req.method;
  msg[5] = this._codec.encode(req.args);

  var i = 6;
  if (req.stdout.listeners('frame').length !== 0) {
    msg[i] = new Buffer('Stdout-Tag');
    var tag = this._nextSeq()
      , tagBuffer = new Buffer(2);
    tagBuffer.writeUInt16BE(tag, 0);
    msg[i+1] = tagBuffer;
    i += 2;
  }
  if (req.stderr.listeners('frame').length !== 0) {
    msg[i] = new Buffer('Stderr-Tag');
    var tag = this._nextSeq()
      , tagBuffer = new Buffer(2);
    tagBuffer.writeUInt16BE(tag, 0);
    msg[i+1] = tagBuffer;
  }

  this._send(msg);
};

/*
 * Socket._sendInterrupt
 */

Socket._sendInterrupt = function _sendInterrupt(reqId) {
  var reqIdBuffer = new Buffer(2);
  reqIdBuffer.writeUInt16BE(reqId, 0);

  this._send([
      FRAME_EMPTY,
      FRAME_HEADER,
      FRAME_MSGTYPE_INTERRUPT,
      reqIdBuffer
  ]);
};

/*
 * Socket._sendProgress
 */

Socket._sendProgress = function _sendProgress(requester, reqId) {
  var reqIdBuffer = new Buffer(2);
  reqIdBuffer.writeUInt16BE(reqId, 0);

  this._send([
      requester,
      FRAME_HEADER,
      FRAME_MSGTYPE_PROGRESS,
      reqIdBuffer
  ]);
};

/*
 * Socket._sendStreamFrame
 */

Socket._sendStreamFrame = function _sendStreamFrame(receiver, streamTag, seq, body) {
  var streamTagBuffer = new Buffer(2);
  streamTagBuffer.writeUInt16BE(streamTag, 0);

  var seqBuffer = new Buffer(2);
  seqBuffer.writeUInt16BE(seq, 0);

  this._send([
      receiver,
      FRAME_HEADER,
      FRAME_MSGTYPE_STREAM_FRAME,
      streamTagBuffer,
      seqBuffer,
      body
  ]);
};

/*
 * Socket._sendReply
 */

Socket._sendReply = function _sendReply(requester, reqId, returnCode, returnValue) {
  var reqIdBuffer = new Buffer(2);
  reqIdBuffer.writeUInt16BE(reqId, 0);

  var returnCodeBuffer = new Buffer(1);
  returnCodeBuffer.writeUInt8(returnCode, 0);

  this._send([
      requester,
      FRAME_HEADER,
      FRAME_MSGTYPE_REPLY,
      reqIdBuffer,
      returnCodeBuffer,
      this._codec.encode(returnValue)
  ]);
};

/*
 * Socket._sendSubscribe
 */

Socket._sendSubscribe = function _sendSubscribe(eventKind) {
  this._send([
      eventKind,
      FRAME_HEADER,
      FRAME_MSGTYPE_SUBSCRIBE
  ]);
};

/*
 * Socket._sendUnsubscribe
 */

Socket._sendUnsubscribe = function _sendUnsubscribe(eventKind) {
  this._send([
      eventKind,
      FRAME_HEADER,
      FRAME_MSGTYPE_UNSUBSCRIBE
  ]);
};

/*
 * Socket._sendEvent
 */

Socket._sendEvent = function _sendEvent(eventKind, eventBody) {
  this._send([
      eventKind,
      FRAME_HEADER,
      FRAME_MSGTYPE_EVENT,
      this._codec.encode(eventBody)
  ]);
};

/*
 * Socket._sendPong
 */

Socket._sendPong = function _sendPong() {
  this._rpcSock.send([
      FRAME_EMPTY,
      FRAME_HEADER,
      FRAME_MSGTYPE_PONG
  ]);
};

/*
 * Socket._send
 */

Socket._send = function _send(frames) {
  if (this._state !== STATE_CONNECTED) throw new Error("Socket state invalid");
  this._rpcSock.send(frames);
};

/*
 * Socket._handleRPCMessage
 */

Socket._handleRPCMessage = function _handleRPCMessage() {
  var msg = arguments;

  if (msg.length < 3) return;
  if (msg[1].toString() !== HEADER) return;
  if (msg[2].length !== 1) return;
  
  switch (msg[2][0]) {
    case MSGTYPE_OHAI:
      if (msg.length % 2 !== 1) {
        next(new Error("invalid OHAI message received"));
        return;
      }

      var opts = {};
      for (i = 3; i < msg.length; i += 2) {
        opts[msg[i]] = msg[i+1];
      }

      msgHandler.handleOhai(opts, next);
      break;
    case MSGTYPE_REQUEST:
      if (msg.length < 6 || msg.length % 2 !== 0) return;

      var requester = msg[0].toString()
        , reqId
        , method = msg[4].toString()
        , args
        , opts = {};
      try {
        reqId = msg[3].readUInt16BE(0);
        args = session._codec.decode(msg[5]);
      } catch (err) {
        return;
      }
      for (i = 6; i < msg.length; i += 2) {
        opts[msg[i]] = msg[i+1];
      }

      inboundRequestsService.handleRequest(requester, reqId, method, args, opts);
      break;
    case MSGTYPE_INTERRUPT:
      if (msg.length != 4) return;

      var requester = msg[0].toString()
        , reqId;
      try {
        reqId = msg[3].readUInt16BE(0);
      } catch (err) {
        return;
      }

      inboundRequestsService.handleInterrupt(requester, reqId);
      break;
    case MSGTYPE_PROGRESS:
      if (msg.length != 4) return;

      var reqId;
      try {
        reqId = msg[3].readUInt16BE(0);
      } catch (err) {
        return;
      };

      msgHandler.emit('PROGRESS', reqId);
      break;
   case MSGTYPE_STREAM_FRAME:
      if (msg.length != 6) return;

      var streamTag
        , seq
        , body = msg[5];
      try {
        streamTag = msg[3].readUInt16BE(0);
        seq = msg[4].readUInt16BE(0);
      } catch (err) {
        return;
      }

      msgHandler.emit('STREAM_FRAME', streamTag, seq, body);
      break;
    case MSGTYPE_REPLY:
      if (msg.length != 6) return;

      var reqId
        , returnCode
        , returnValue;
      try {
        reqId = msg[3].readUInt16BE(0);
        returnCode = msg[4].readUInt8(0);
        returnValue = session._codec.decode(msg[5]);
      } catch (err) {
        return;
      }

      msgHandler.emit('REPLY', reqId, returnCode, returnValue);
      break;
    case MSGTYPE_SUBSCRIBE_ACK:
      if (msg.length != 5) return;

      var kind = msg[3].toString()
        , seq;
      try {
        seq = msg[4].readUInt16BE(0);
      } catch (err) {
        return;
      }

      eventService.handleSubscribeAck(kind, seq);
      break;
    case MSGTYPE_PING:
      msgHandler.emit('PING');
      break;
  }

};

/*
 * Socket._handleRPCError
 */

Socket._handleRPCError = function _handleRPCError() {

};

/*
 * Socket._handlePUBMessage
 */

Socket._handlePUBMessage = function _handleRPCMessage() {
  var msg = arguments;

  if (msg.length !== 6) return;
  if (msg[2].toString() != HEADER) return;
  if (msg[3].length !== 1 || msg[3][0] != MSGTYPE_EVENT) return;

  var kind = msg[0].toString()
    , body = {}
    , seq;

  try {
    if (msg[4].length !== 0) body = session._codec.decode(msg[4]);
    seq = msg[5].readUInt16BE(0);
  } catch (err) {
    return;
  }

  eventService.handleEvent(kind, seq, body);

};

/*
 * Socket._handlePUBError
 */


Socket._handlePUBError = function _handleRPCMessage() {

};

/*
 * Socket._handleOhai
 */

Socket._handleOhai = function _handleOhai(opts) {
  var encoding = 'json';
  if (opts['Encoding'] !== undefined) {
    encoding = opts['Encoding'].toString();
  }

  try {
    this._codec = require('./codec').codec(encoding);
  } catch (err) {
    this._close();
    this._next(err);
    return;
  }

  var session = require('./session').createSession(this);

  this._state = STATE_CONNECTED;
  this._next(null, session);
};

/*
 * Socket._handlePing
 */

Socket._handlePing = function handlePing() {
  this._sendPong();
};
