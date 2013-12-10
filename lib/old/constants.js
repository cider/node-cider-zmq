//------------------------------------------------------------------------------
// Constants
//------------------------------------------------------------------------------

const HEADER                = 'CDR01'
    , MSGTYPE_HELLO         = 0x00
    , MSGTYPE_OHAI          = 0x01
    , MSGTYPE_KTHXBYE       = 0x02
    , MSGTYPE_REGISTER      = 0x03
    , MSGTYPE_UNREGISTER    = 0x04
    , MSGTYPE_REQUEST       = 0x05
    , MSGTYPE_INTERRUPT     = 0x06
    , MSGTYPE_PROGRESS      = 0x07
    , MSGTYPE_STREAM_FRAME  = 0x08
    , MSGTYPE_REPLY         = 0x09
    , MSGTYPE_SUBSCRIBE     = 0x0A
    , MSGTYPE_SUBSCRIBE_ACK = 0x0B
    , MSGTYPE_UNSUBSCRIBE   = 0x0C
    , MSGTYPE_EVENT         = 0x0D
    , MSGTYPE_PING          = 0x0E
    , MSGTYPE_PONG          = 0x0F;

const FRAME_EMPTY                 = new Buffer(0)
    , FRAME_HEADER                = new Buffer(HEADER)
    , FRAME_MSGTYPE_HELLO         = new Buffer([MSGTYPE_HELLO])
    , FRAME_MSGTYPE_OHAI          = new Buffer([MSGTYPE_OHAI])
    , FRAME_MSGTYPE_KTHXBYE       = new Buffer([MSGTYPE_KTHXBYE])
    , FRAME_MSGTYPE_REGISTER      = new Buffer([MSGTYPE_REGISTER])
    , FRAME_MSGTYPE_UNREGISTER    = new Buffer([MSGTYPE_UNREGISTER])
    , FRAME_MSGTYPE_REQUEST       = new Buffer([MSGTYPE_REQUEST])
    , FRAME_MSGTYPE_INTERRUPT     = new Buffer([MSGTYPE_INTERRUPT])
    , FRAME_MSGTYPE_PROGRESS      = new Buffer([MSGTYPE_PROGRESS])
    , FRAME_MSGTYPE_STREAM_FRAME  = new Buffer([MSGTYPE_STREAM_FRAME])
    , FRAME_MSGTYPE_REPLY         = new Buffer([MSGTYPE_REPLY])
    , FRAME_MSGTYPE_SUBSCRIBE     = new Buffer([MSGTYPE_SUBSCRIBE])
    , FRAME_MSGTYPE_SUBSCRIBE_ACK = new Buffer([MSGTYPE_SUBSCRIBE_ACK])
    , FRAME_MSGTYPE_UNSUBSCRIBE   = new Buffer([MSGTYPE_UNSUBSCRIBE])
    , FRAME_MSGTYPE_EVENT         = new Buffer([MSGTYPE_EVENT])
    , FRAME_MSGTYPE_PING          = new Buffer([MSGTYPE_PING])
    , FRAME_MSGTYPE_PONG          = new Buffer([MSGTYPE_PONG]);


