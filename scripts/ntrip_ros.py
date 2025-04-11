#!/usr/bin/env python

import os
import sys
import json
import importlib.util
from time import sleep

import rclpy
from rclpy.node import Node
from std_msgs.msg import Header, String
from nmea_msgs.msg import Sentence

from ntrip_client.ntrip_client import NTRIPClient
from ntrip_client.nmea_parser import NMEA_DEFAULT_MAX_LENGTH, NMEA_DEFAULT_MIN_LENGTH

# Try to import a couple different types of RTCM messages
_MAVROS_MSGS_NAME = "mavros_msgs"
_RTCM_MSGS_NAME = "rtcm_msgs"
have_mavros_msgs = False
have_rtcm_msgs = False
if importlib.util.find_spec(_MAVROS_MSGS_NAME) is not None:
  have_mavros_msgs = True
  from mavros_msgs.msg import RTCM as mavros_msgs_RTCM
if importlib.util.find_spec(_RTCM_MSGS_NAME) is not None:
  have_rtcm_msgs = True
  from rtcm_msgs.msg import Message as rtcm_msgs_RTCM

class NTRIPRos(Node):
  def __init__(self):
    # Read a debug flag from the environment that should have been set by the launch file
    try:
      self._debug = json.loads(os.environ["NTRIP_CLIENT_DEBUG"].lower())
    except:
      self._debug = False

    # Init the node and declare params
    super().__init__('ntrip_client')
    self.declare_parameters(
      namespace='',
      parameters=[
        ('host', '127.0.0.1'),
        ('port', 2101),
        ('mountpoint', 'mount'),
        ('ntrip_version', 'None'),
        ('ntrip_server_hz', 1.0), # set to max at 1hz for rtk2go
        ('authenticate', False),
        ('username', ''),
        ('password', ''),
        ('ssl', False),
        ('cert', 'None'),
        ('key', 'None'),
        ('ca_cert', 'None'),
        ('rtcm_frame_id', 'odom'),
        ('nmea_max_length', NMEA_DEFAULT_MAX_LENGTH),
        ('nmea_min_length', NMEA_DEFAULT_MIN_LENGTH),
        ('rtcm_message_package', _MAVROS_MSGS_NAME),
        ('reconnect_attempt_max', NTRIPClient.DEFAULT_RECONNECT_ATTEMPT_MAX),
        ('reconnect_attempt_wait_seconds', NTRIPClient.DEFAULT_RECONNECT_ATEMPT_WAIT_SECONDS),
        ('rtcm_timeout_seconds', NTRIPClient.DEFAULT_RTCM_TIMEOUT_SECONDS),
      ]
    )

    # Read some mandatory config
    host = self.get_parameter('host').value
    port = self.get_parameter('port').value
    mountpoint = self.get_parameter('mountpoint').value

    # Optionally get the ntrip version from the launch file
    ntrip_version = self.get_parameter('ntrip_version').value
    if ntrip_version == 'None':
      ntrip_version = None

    # Set the rate at which RTCM requests and NMEA messages are sent
    self.rtcm_request_rate = 1.0 / self.get_parameter('ntrip_server_hz').value

    # Initialize variables to store the most recent NMEA message
    self._latest_nmea = None

    # Set the log level to debug if debug is true
    if self._debug:
      rclpy.logging.set_logger_level(self.get_logger().name, rclpy.logging.LoggingSeverity.DEBUG)

    # If we were asked to authenticate, read the username and password
    username = None
    password = None
    if self.get_parameter('authenticate').value:
      username = self.get_parameter('username').value
      password = self.get_parameter('password').value
      if not username:
        self.get_logger().error(
          'Requested to authenticate, but param "username" was not set')
        sys.exit(1)
      if not password:
        self.get_logger().error(
          'Requested to authenticate, but param "password" was not set')
        sys.exit(1)

    # Read an optional Frame ID from the config
    self._rtcm_frame_id = self.get_parameter('rtcm_frame_id').value

    # Determine the type of RTCM message that will be published
    rtcm_message_package = self.get_parameter('rtcm_message_package').value
    if rtcm_message_package == _MAVROS_MSGS_NAME:
      if have_mavros_msgs:
        self._rtcm_message_type = mavros_msgs_RTCM
        self._create_rtcm_message = self._create_mavros_msgs_rtcm_message
      else:
        self.get_logger().fatal('The requested RTCM package {} is a valid option, but we were unable to import it. Please make sure you have it installed'.format(rtcm_message_package))
    elif rtcm_message_package == _RTCM_MSGS_NAME:
      if have_rtcm_msgs:
        self._rtcm_message_type = rtcm_msgs_RTCM
        self._create_rtcm_message = self._create_rtcm_msgs_rtcm_message
      else:
        self.get_logger().fatal('The requested RTCM package {} is a valid option, but we were unable to import it. Please make sure you have it installed'.format(rtcm_message_package))
    else:
      self.get_logger().fatal('The RTCM package {} is not a valid option. Please choose between the following packages {}'.format(rtcm_message_package, ','.join([_MAVROS_MSGS_NAME, _RTCM_MSGS_NAME])))

    # Setup the RTCM publisher
    self._rtcm_pub = self.create_publisher(self._rtcm_message_type, 'rtcm', 10)

    # Setup a server frequency confirmation publisher
    self._rate_confirm_pub = self.create_publisher(String, 'ntrip_server_hz', 10)

    # Initialize the client
    self._client = NTRIPClient(
      host=host,
      port=port,
      mountpoint=mountpoint,
      ntrip_version=ntrip_version,
      username=username,
      password=password,
      logerr=self.get_logger().error,
      logwarn=self.get_logger().warning,
      loginfo=self.get_logger().info,
      logdebug=self.get_logger().debug
    )

    # Get some SSL parameters for the NTRIP client
    self._client.ssl = self.get_parameter('ssl').value
    self._client.cert = self.get_parameter('cert').value
    self._client.key = self.get_parameter('key').value
    self._client.ca_cert = self.get_parameter('ca_cert').value
    if self._client.cert == 'None':
      self._client.cert = None
    if self._client.key == 'None':
      self._client.key = None
    if self._client.ca_cert == 'None':
      self._client.ca_cert = None

    # Get some timeout parameters for the NTRIP client
    self._client.nmea_parser.nmea_max_length = self.get_parameter('nmea_max_length').value
    self._client.nmea_parser.nmea_min_length = self.get_parameter('nmea_min_length').value
    self._client.reconnect_attempt_max = self.get_parameter('reconnect_attempt_max').value
    self._client.reconnect_attempt_wait_seconds = self.get_parameter('reconnect_attempt_wait_seconds').value
    self._client.rtcm_timeout_seconds = self.get_parameter('rtcm_timeout_seconds').value

  def run(self):
    # Connect the client
    if not self._client.connect():
      self.get_logger().error('Unable to connect to NTRIP server')
      return False
    
    # Setup the subscriber for NMEA data
    self._nmea_sub = self.create_subscription(Sentence, 'nmea', self.subscribe_nmea, 10)

    # Start the timer that will send both RTCM requests and NMEA data at the configured rate
    self._rtcm_timer = self.create_timer(self.rtcm_request_rate, self.send_rtcm_and_nmea)
    
    return True

  def stop(self):
    self.get_logger().info('Stopping RTCM publisher')
    if self._rtcm_timer:
      self._rtcm_timer.cancel()
      self._rtcm_timer.destroy()
    self.get_logger().info('Disconnecting NTRIP client')
    self._client.disconnect()
    self.get_logger().info('Shutting down node')
    self.destroy_node()

  def subscribe_nmea(self, nmea):
    # Cache the latest NMEA sentence
    # TODO: polish this or fix at driver only sending GNGGA sentences
    # Temporarily fix LG RTK Provider problem by using only GNGGA sentences
    # Not sure if this is the right thing to do, but python will escape the return characters at the end of the string, so do this manually
    temp_sentence = nmea.sentence
    if temp_sentence[-4:] == '\\r\\n':
      temp_sentence = temp_sentence[:-4] + '\r\n'
    elif temp_sentence[-2:] != '\r\n':
      temp_sentence = temp_sentence + '\r\n'
    if not str(temp_sentence).startswith('$GNGGA'):
      return
    self._latest_nmea = nmea.sentence

  def send_rtcm_and_nmea(self):
    # Send cached NMEA data if available
    if self._latest_nmea is not None:
      self._client.send_nmea(self._latest_nmea)

    # Request and publish RTCM data
    for raw_rtcm in self._client.recv_rtcm():
      self._rtcm_pub.publish(self._create_rtcm_message(raw_rtcm))

    # Publish a confirmation message to indicate the send_rtcm_and_nmea call
    confirmation_msg = String()
    confirmation_msg.data = "RTCM request and NMEA receive set at rate: {} Hz".format(1.0 / self.rtcm_request_rate)
    self._rate_confirm_pub.publish(confirmation_msg)

  def _create_mavros_msgs_rtcm_message(self, rtcm):
    return mavros_msgs_RTCM(
      header=Header(
        stamp=self.get_clock().now().to_msg(),
        frame_id=self._rtcm_frame_id
      ),
      data=rtcm
    )

  def _create_rtcm_msgs_rtcm_message(self, rtcm):
    return rtcm_msgs_RTCM(
      header=Header(
        stamp=self.get_clock().now().to_msg(),
        frame_id=self._rtcm_frame_id
      ),
      message=rtcm
    )

if __name__ == '__main__':
  # Start the node
  rclpy.init()
  node = NTRIPRos()

  while not node.run():
    sleep(1) # Wait a second before trying again

  try:
    # Spin until we are shut down
    rclpy.spin(node)
  except KeyboardInterrupt:
    pass
  except BaseException as e:
    raise e
  finally:
    node.stop()
    
    # Shutdown the node and stop rclpy
    rclpy.shutdown()