<?php
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * @package thrift.transport
 */
namespace Thrift\Transport;

use Thrift\Exception\TException;
use Thrift\Exception\TTransportException;
use Thrift\Factory\TStringFuncFactory;

/**
 * Sockets implementation of the TTransport interface.
 *
 * @package thrift.transport
 */
class TPhpSocket extends TTransport {
    /**
     * Handle to PHP socket
     *
     * @var resource
     */
    protected $_handle = null;

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
     * Sock type
     *
     * @var int
     */
    protected $_sockType = AF_INET;

    /**
     * Send timeout in seconds.
     *
     * Combined with sendTimeoutUsec this is used for send timeouts.
     *
     * @var int
     */
    protected $_sendTimeoutSec = 0;

    /**
     * Send timeout in microseconds.
     *
     * Combined with sendTimeoutSec this is used for send timeouts.
     *
     * @var int
     */
    protected $_sendTimeoutUsec = 100000;

    /**
     * Recv timeout in seconds
     *
     * Combined with recvTimeoutUsec this is used for recv timeouts.
     *
     * @var int
     */
    protected $_recvTimeoutSec = 0;

    /**
     * Recv timeout in microseconds
     *
     * Combined with recvTimeoutSec this is used for recv timeouts.
     *
     * @var int
     */
    protected $_recvTimeoutUsec = 750000;

    /**
     * Debugging on?
     *
     * @var bool
     */
    protected $_debug = false;

    /**
     * Debug handler
     *
     * @var mixed
     */
    protected $_debugHandler = null;

    /**
     * Socket constructor
     *
     * @param string $host         Remote hostname
     * @param int $port            Remote port
     * @param int $sockType        Sock type
     * @param string $debugHandler Function to call for error logging
     */
    public function __construct($host = 'localhost', $port = 9090, $sockType = AF_INET, $debugHandler = null) {
        $this->_host = $host;
        $this->_port = $port;
        $this->_sockType = $sockType;
        $this->_debugHandler = $debugHandler ? $debugHandler : 'error_log';
    }

    /**
     * @param resource $handle
     *
     * @return void
     */
    public function setHandle($handle) {
        $this->_handle = $handle;
    }

    /**
     * Sets the send timeout.
     *
     * @param int $timeout Timeout in milliseconds.
     */
    public function setSendTimeout($timeout) {
        $this->_sendTimeoutSec = floor($timeout/1000);
        $this->_sendTimeoutUsec = ($timeout - ($this->_sendTimeoutSec*1000))*1000;
    }

    /**
     * Sets the receive timeout.
     *
     * @param int $timeout Timeout in milliseconds.
     */
    public function setRecvTimeout($timeout) {
        $this->_recvTimeoutSec = floor($timeout/1000);
        $this->_recvTimeoutUsec = ($timeout - ($this->_recvTimeoutSec*1000))*1000;
    }

    /**
     * Sets debugging output on or off
     *
     * @param bool $debug
     */
    public function setDebug($debug) {
        $this->_debug = $debug;
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
     *
     * @return boolean true if open
     */
    public function isOpen() {
        return is_resource($this->_handle);
    }

    /**
     * Connects the socket.
     */
    public function open() {
        if($this->isOpen()) {
            return;
        }

        if (!$this->_handle) {
            switch ($this->_sockType) {
                case AF_INET:
                    if(empty($this->_host)) {
                        throw new TTransportException(self::class . ': Cannot open null host', TTransportException::NOT_OPEN);
                    }

                    if($this->_port <= 0) {
                        throw new TTransportException(self::class . ': Cannot open without port', TTransportException::NOT_OPEN);
                    }

                    $this->_handle = @socket_create(AF_INET, SOCK_STREAM, SOL_TCP);
                    break;

                case AF_UNIX:
                    if(empty($this->_host)) {
                        throw new TTransportException(self::class . ': Cannot open without unix socket file', TTransportException::NOT_OPEN);
                    }

                    $this->_port = 0;
                    $this->_handle = @socket_create(AF_UNIX, SOCK_STREAM, 0);
                    break;

                default:
                    throw new TException(self::class . ': Wrong sock type.' . $this->_sockType);
            }

            // Create Failed?
            if($this->_handle === false) {
                $error = self::class . ': Failed to create to ' . $this->getDomain();

                if($this->_debug) {
                    call_user_func($this->_debugHandler, $error);
                }

                throw new TException($error);
            }

            if(@socket_set_option($this->_handle, SOL_SOCKET, SO_RCVTIMEO,
                    ['sec' => $this->_recvTimeoutSec, 'usec' => $this->_recvTimeoutUsec]) === false) {

                $this->error(self::class . ': Failed to set recv timeout option');
            }

            if(@socket_set_option($this->_handle, SOL_SOCKET, SO_SNDTIMEO,
                    ['sec' => $this->_sendTimeoutSec, 'usec' => $this->_sendTimeoutUsec]) === false) {

                $this->error(self::class . ': Failed to set send timeout option');
            }
        }

        if(@socket_connect($this->_handle, $this->_host, $this->_port) === false) {
            $this->error(self::class . ': Failed to connect to ' . $this->getDomain());
        }
    }

    /**
     * Reconnect the socket
     */
    public function reconnect() {
        if(is_resource($this->_handle)) {
            @socket_shutdown($this->_handle);
            @socket_close($this->_handle);
        }

        $this->open();
    }

    /**
     * Close the transport.
     */
    public function close() {
        if(is_resource($this->_handle)) {
            @socket_shutdown($this->_handle);
            @socket_close($this->_handle);
        }

        $this->_handle = null;
    }

    /**
     * Read some data into the array.
     *
     * @param int $len How much to read
     *
     * @return string The data that has been read
     * @throws TTransportException if cannot read any more data
     */
    public function read($len) {
        $read = 0;
        $data = '';

        while($read < $len) {
            $fread = @socket_read($this->_handle, $len - $read, PHP_BINARY_READ);

            if($fread === false) {
                $errer_no = socket_last_error($this->_handle);

                if($errer_no == SOCKET_EINTR) {
                    usleep(1);
                    continue;
                } else {
                    $this->error(self::class . ': Failed to read data from ' . $this->getDomain(), $errer_no);
                }
            } elseif($fread == '') {
                //Connection closed
                $this->error(self::class . ': TSocket read 0 bytes from ' . $this->getDomain(), SOCKET_ECONNRESET);
            }

            $read += TStringFuncFactory::create()->strlen($fread);
            $data .= $fread;
        }

        return $data;
    }

    /**
     * Writes the given data out.
     *
     * @param string $buf The data to write
     *
     * @throws TTransportException if writing fails
     */
    public function write($buf) {
        $written = 0;
        $length = TStringFuncFactory::create()->strlen($buf);

        while($written < $length) {
            $fwrite = @socket_write($this->_handle, TStringFuncFactory::create()->substr($buf, $written));

            if($fwrite === false) {
                $this->error(self::class . ': Failed to write buffer to ' . $this->getDomain());
            }

            $written += $fwrite;
        }
    }

    /**
     * Flush output to the socket.
     *
     * Since read(), readAll() and write() operate on the sockets directly,
     * this is a no-op
     *
     * If you wish to have flushable buffering behaviour, wrap this TSocket
     * in a TBufferedTransport.
     */
    public function flush() {
        // no-op
    }

    /**
     * Fail with socket error
     *
     * @param string $msg
     * @param int $errer_no
     *
     * @throws TTransportException
     */
    private function error($msg, $errer_no = 0) {
        $errer_no = $errer_no ?: socket_last_error($this->_handle);
        $msg .= ', ' . socket_strerror($errer_no);

        throw new TTransportException($msg, $errer_no);
    }
}
