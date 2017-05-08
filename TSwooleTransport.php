<?php
/**
 * Swoole framed transport
 *
 * @author Janson
 * @create 2017-03-29
 */
namespace Thrift\Transport;

use Thrift\Exception\TException;
use Thrift\Exception\TTransportException;

/**
 * Sockets implementation of the TTransport interface.
 *
 * @package thrift.transport
 */
class TSwooleTransport extends TTransport {
    /**
     * Swoole client socket
     *
     * @var \swoole_client
     */
    protected $_socket;

    /**
     * Remote hostname
     *
     * @var string
     */
    protected $_host = 'localhost';

    /**
     * Remote port
     *
     * @var int
     */
    protected $_port = 9090;

    /**
     * Swoole sock type
     *
     * @var int
     */
    protected $_sockType = SWOOLE_TCP;

    /**
     * Persistent socket or plain?
     *
     * @var bool
     */
    protected $_persist = false;

    /**
     * Timeout in seconds
     *
     * @var int
     */
    protected $_timeout = 3;

    /**
     * Client setting
     * @var array
     */
    protected $_setting = [
        'open_length_check'     => true,
        'package_max_length'    => 4096000,
        'package_length_type'   => 'N',
        'package_length_offset' => 0,
        'package_body_offset'   => 4
    ];

    /**
     * Buffer for read data.
     *
     * @var string
     */
    protected $_rBuf;

    /**
     * Buffer for queued output data
     *
     * @var string
     */
    protected $_wBuf;

    /**
     * TSwooleTransport constructor.
     *
     * @param string $host  Remote hostname
     * @param int $port     Remote port
     * @param int $sockType Swoole sock type
     * @param bool $persist Whether to use a persistent socket
     */
    public function __construct($host = 'localhost', $port = 9090, $sockType = SWOOLE_TCP, $persist = false) {
        $this->_host = $host;
        $this->_port = $port;
        $this->_sockType = $sockType;
        $this->_persist = $persist;
    }

    /**
     * Set connect/recv/send timeout
     * @param int $timeout
     * @return bool
     */
    public function setTimeout($timeout) {
        if ($timeout <= 0) {
            return false;
        }

        $this->_timeout = $timeout;
        return true;
    }

    /**
     * Set client setting
     * @param array $setting
     * @return bool
     */
    public function setSetting($setting) {
        if (empty($setting)) {
            return false;
        }

        $this->_setting = $setting;
        return true;
    }

    /**
     * Get the domain that this socket is connected to
     *
     * @return string
     */
    public function getDomain() {
        return $this->_port > 0 ? "{$this->_host}:{$this->_port}" : $this->_host;
    }

    /**
     * Whether this transport is open.
     * @return boolean true if open
     */
    public function isOpen() {
        if (!$this->_socket) {
            return false;
        }

        return $this->_socket->isConnected();
    }

    /**
     * Open the transport for reading/writing
     * @throws TTransportException if cannot open
     */
    public function open() {
        if($this->isOpen()) {
            return;
        }

        if (!$this->_socket) {
            switch ($this->_sockType) {
                case SWOOLE_TCP:
                    if(empty($this->_host)) {
                        throw new TTransportException(self::class . ': Cannot open null host', TTransportException::NOT_OPEN);
                    }

                    if($this->_port <= 0) {
                        throw new TTransportException(self::class . ': Cannot open without port', TTransportException::NOT_OPEN);
                    }

                    $this->_socket = new \swoole_client($this->_persist ? SWOOLE_TCP | SWOOLE_KEEP : SWOOLE_TCP);
                    break;

                case SWOOLE_UNIX_STREAM:
                    if(empty($this->_host)) {
                        throw new TTransportException(self::class . ': Cannot open without unix socket file', TTransportException::NOT_OPEN);
                    }

                    $this->_port = 0;
                    $this->_socket = new \swoole_client(SWOOLE_UNIX_STREAM);
                    break;

                default:
                    throw new TException(self::class . ': Wrong sock type.' . $this->_sockType);
            }

            //Client setting
            $this->_socket->set($this->_setting);
        }

        //Connect failed?
        if(@$this->_socket->connect($this->_host, $this->_port, $this->_timeout) === false) {
            if ($this->_persist) {
                $this->_socket->close(true);
            }

            $this->error(self::class . ': Failed to connect to ' . $this->getDomain());
        }
    }

    /**
     * Reconnect the socket
     */
    public function reconnect() {
        if ($this->_socket) {
            @$this->_socket->close($this->_persist);
        }

        $this->open();
    }

    /**
     * Close the transport.
     */
    public function close() {
        if ($this->_socket) {
            @$this->_socket->close($this->_persist);
        }

        $this->_socket = null;
    }

    /**
     * Read some data into the array.
     *
     * @param int $len How much to read
     * @return string The data that has been read
     * @throws TTransportException if cannot read any more data
     */
    public function read($len) {
        if ($this->_rBuf === null) {
            $this->readFrame();
        }

        // Just return full buff
        if ($len >= strlen($this->_rBuf)) {
            $out = $this->_rBuf;
            $this->_rBuf = null;

            return $out;
        }

        $out = substr($this->_rBuf, 0, $len);
        $this->_rBuf = substr($this->_rBuf, $len);

        return $out;
    }

    /**
     * Reads a chunk of data into the internal read buffer.
     */
    private function readFrame() {
        $data = @$this->_socket->recv();

        if ($data === false) {
            $this->error(self::class . ': Failed to read from ' . $this->getDomain());
        } elseif ($data === '') {
            $this->error(self::class . ': TSwoole read 0 bytes from ' . $this->getDomain(), SOCKET_ECONNRESET);
        }

        $this->_rBuf = substr($data, 4);
    }

    /**
     * Writes the given data out.
     *
     * @param string $buf The data to write
     * @throws TTransportException if writing fails
     */
    public function write($buf) {
        $this->_wBuf .= $buf;
    }

    /**
     * Writes the output buffer to the stream in the format of a 4-byte length
     * followed by the actual data.
     */
    public function flush() {
        $out = pack('N', strlen($this->_wBuf));
        $out .= $this->_wBuf;

        if (@$this->_socket->send($out) === false) {
            $this->error(self::class . ': Failed to write buffer to ' . $this->getDomain());
        }

        $this->_wBuf = '';
    }

    /**
     * Fail with socket error
     *
     * @param string $msg
     * @param int $errer_no
     * @throws TTransportException
     */
    private function error($msg, $errer_no = 0) {
        $errer_no = $errer_no ?: $this->_socket->errCode;
        $msg .= ', ' . socket_strerror($errer_no);

        throw new TTransportException($msg, $errer_no);
    }
}
