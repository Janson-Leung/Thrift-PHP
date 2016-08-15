<?php
/**
 * Thrift Stream 客户端
 * @author Janson
 * @create 2016-04-20
 */

namespace App\Helper\Thrift;

use App\Helper\Log;
use \Thrift\Transport\TSocket;
use \Thrift\Transport\TFramedTransport;
use \Thrift\Protocol\TBinaryProtocol;
use \Thrift\Exception\TTransportException;

class ThriftClient {
	private $_host;
	private $_port;
	private $_module;
	private $_service;
	private $_transport;
	private $_tsh = null;			//thrift socket handle
	private $_retryTimes = 1;		//重试次数
	private $_sendTimeout = 10000;	//发送时长：毫秒
	private $_recvTimeout = 20000;	//接收时长

	public $logPath = SERVICE_LOG_PATH . 'thrift/';

	private static $_userId;
	private static $_corpId;
	private static $_instances = array();

	private function __construct($module) {
		$this->_module = $module;
		$this->getThriftSocket();	//获取thrift socket实例
	}

	/**
	 * 获取Thrift客户端模块实例
	 * @param string $module	Thrift服务模块key
	 * @param int $userId		员工ID
	 * @param int $corpId		企业ID
	 * @return mixed
	 * @throws \Exception
	 */
	public static function getInstance($module, $userId, $corpId) {
		try {
			if (empty($module)) {
				throw new \Exception('ThriftClient: Thrift client module key can not be empty');
			}

			if($userId > 0 && $corpId > 0) {
				self::$_userId = $userId;
				self::$_corpId = $corpId;
			}
			else {
				throw new \Exception('ThriftClient: userId or corpId is wrong');
			}

			if ( ! isset(self::$_instances[$module])) {
				self::$_instances[$module] = new self($module);
			}
		} catch(\Exception $e) {
			Log::error(
				'ThriftClient',
				SERVICE_LOG_PATH . 'thrift/error.log',
				$e->getMessage(),
				array(
					'module' => $module,
					'userId' => $userId,
					'corpId' => $corpId
				)
			);

			throw $e;
		}

		return self::$_instances[$module];
	}

	/**
	 * 客户端方法调用
	 * @param $method
	 * @param $arguments
	 * @return array
	 * @throws \Exception
	 */
	public function __call($method, $arguments) {
		$requestId = uniqid();

		try{
			$clientInfo = array(
				'module' => $this->_module,
				'userId' => self::$_userId,
				'corpId' => self::$_corpId,
				'_host' => $this->_host,
				'port' => $this->_port,
				'service' => $this->_service,
				'method' => $method,
				'arguments' => $arguments
			);

			$start = microtime(true);
			for($i = 0; $i <= $this->_retryTimes; $i++) {
				try{
					$callback = array($this->_tsh, $method);
					if ( ! is_callable($callback)) {
						throw new \Exception("ThriftClient: {$this->_service}Client->{$method} is not callable");
					}

					$json = call_user_func_array($callback, $arguments);    //调用客户端方法
					$result = json_decode($json, true);
					if ($result) {
						$clientInfo['RequestTime'] = microtime(true) - $start;

						Log::info(
							$this->_service . 'Client',
							$this->logPath . 'request.log',
							"RequestId: $requestId\t" . json_encode($clientInfo),
							$result
						);

						break;
					}
					else {
						throw new \Exception('ThriftClient: ' . json_last_error_msg() . "(json: $json)");
					}
				} catch(TTransportException $e) {
					$this->reconnect();

					if($i < $this->_retryTimes) {
						Log::error(
							$this->_service . 'Client',
							$this->logPath . 'retry.log',
							"RequestId: $requestId, Times: " . ($i + 1) . ", Error: {$e->getMessage()}",
							$clientInfo
						);
					}
					else{
						throw $e;
					}
				}
			}
		} catch(\Exception $e) {
			Log::error(
				$this->_service . 'Client',
				$this->logPath . 'error.log',
				"RequestId: $requestId, Error: " . $e->getMessage(),
				$clientInfo
			);

			throw $e;
		}

		return $result;
	}

	/**
	 * Thrift TSocket 重连
	 */
	private function reconnect() {
		if($this->_transport) {
			$this->_transport->close();
			$this->_transport->open();
		}
	}

	/**
	 * 获取一个Thrift Client 实例
	 * @throws \Exception
	 */
	private function getThriftSocket() {
		$this->config();	//加载配置

		// TSocket
		$socket = new TSocket($this->_host, $this->_port);
		$socket->setSendTimeout($this->_sendTimeout);
		$socket->setRecvTimeout($this->_recvTimeout);

		$this->_transport = new TFramedTransport($socket);	//Transport
		$protocol = new TBinaryProtocol($this->_transport);	//Protocol
		$this->_transport->open();

		$className = "\\GenPhp\\{$this->_service}\\{$this->_service}Client";	//客户端类名称
		if( ! class_exists($className)) {
			throw new \Exception("ThriftClient: Class '{$className}' not found in " . __FILE__ . " on line" . __LINE__);
		}

		$this->_tsh = new $className($protocol);	//初始化一个Thrift RPC实例
	}

	/**
	 * 获取Thrift服务模块配置
	 * @throws \Exception
	 */
	private function config() {
		$thrift_config = \Zend_Registry::get('thrift_config');

		if( ! isset($thrift_config[$this->_module])) {
			throw new \InvalidArgumentException('ThriftClient: Thrift client module key is wrong');
		}

		if(empty($thrift_config[$this->_module]) || ! is_array($thrift_config[$this->_module])) {
			throw new \InvalidArgumentException('ThriftClient: Thrift config is empty or not an array');
		}

		if( ! isset($thrift_config[$this->_module]['host']) || ! isset($thrift_config[$this->_module]['port']) || ! isset($thrift_config[$this->_module]['service'])) {
			throw new \InvalidArgumentException('ThriftClient: Thrift _host, port or service is not set');
		}

		$this->_host = $thrift_config[$this->_module]['host'];
		$this->_port = $thrift_config[$this->_module]['port'];
		$this->_service = $thrift_config[$this->_module]['service'];

		if(isset($thrift_config[$this->_module]['log_path'])) {
			$this->logPath = $thrift_config[$this->_module]['log_path'];
		}

		if(isset($thrift_config[$this->_module]['send_timeout'])) {
			$this->_sendTimeout = intval($thrift_config[$this->_module]['send_timeout']);
		}

		if(isset($thrift_config[$this->_module]['recv_timeout'])) {
			$this->_recvTimeout = intval($thrift_config[$this->_module]['recv_timeout']);
		}
	}

	/**
	 * 防止克隆
	 */
	private function __clone() {

	}
}