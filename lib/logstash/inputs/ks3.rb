# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "stud/interval"
require "socket"
require 'net/http'
require 'uri'

require 'yaml'

require 'java'
java_import java.io.InputStream
java_import java.io.InputStreamReader
java_import java.io.FileInputStream
java_import java.io.BufferedReader
java_import java.util.zip.GZIPInputStream
java_import java.util.zip.ZipException
java_import java.util.List;

require 'logstash-input-ks3_jars'
java_import com.ksyun.ks3.AutoAbortInputStream;
java_import com.ksyun.ks3.service.Ks3ClientConfig;
java_import com.ksyun.ks3.http.HttpClientConfig;
java_import com.ksyun.ks3.service.Ks3Client;
java_import com.ksyun.ks3.exception.Ks3ServiceException;
java_import com.ksyun.ks3.dto.Ks3ObjectSummary;
java_import com.ksyun.ks3.dto.ObjectListing;
java_import com.ksyun.ks3.service.request.ListObjectsRequest;
java_import com.ksyun.ks3.service.request.GetObjectRequest;
java_import com.ksyun.ks3.dto.Ks3Object;

class LogStash::Inputs::Ks3 < LogStash::Inputs::Base
  config_name "ks3"

  # If undefined, Logstash will complain, even if codec is unused.
  default :codec, "plain"

  config :interval, :validate => :number, :default => 60

  config :access_key_id, :validate => :string, :default => nil
  config :access_key_secret, :validate => :string, :default => nil
  config :endpoint, :validate => :string, :default => nil
  config :bucket, :validate => :string, :default => nil
  config :marker_file, :validate => :string, :default => File.join(Dir.home, '.ks3-marker.yml')
  config :prefix, :validate => :string, :default => ""

  public
  def register
    @host = Socket.gethostname
    @markerConfig = MarkerConfig.new @marker_file

    @logger.info("Registering ks3 input", :bucket => @bucket, :endpoint => @endpoint)

    clientConfig = Ks3ClientConfig.new()
    clientConfig.setEndpoint("ks3-cn-beijing.ksyun.com")

    hconfig = HttpClientConfig.new()
    clientConfig.setHttpClientConfig(hconfig)

    @ks3client = Ks3Client.new(@access_key_id, @access_key_secret, clientConfig)
    @logger.info("bucket", :bucket => @bucket)

    @listObjectsRequest = ListObjectsRequest.new(@bucket)
    # prefix表示列出的object的key以prefix开始
    @listObjectsRequest.setPrefix(@prefix)
    # 设置最大遍历出多少个对象, 一次listobject最大支持1000
    @listObjectsRequest.setMaxKeys(1000)
    @listObjectsRequest.setMarker(@markerConfig.getMarker)
  end

  def run(queue)
    @current_thread = Thread.current
    Stud.interval(@interval) do
      process(queue)
    end
  end

  def process_test(queue)
     # we can abort the loop if stop? becomes true
    while !stop?
      event = LogStash::Event.new(
        "host" => @host,
        "endpoint"=> @endpoint,
        "access_key_id" => @access_key_id,
        "access_key_secret" => @access_key_secret,
        "bucket" => @bucket
      )
      decorate(event)
      queue << event
      Stud.stoppable_sleep(@interval) { stop? }
    end # loop
  end

  def process(queue)
    @logger.info('Marker from: ' + @markerConfig.getMarker)

    objectListing = @ks3client.listObjects(@listObjectsRequest)
    nextMarker = objectListing.getNextMarker()
    ks3ObjectSummaries = objectListing.getObjectSummaries()
    ks3ObjectSummaries.each do |obj|
       # 文件的路径key
       key = obj.getKey()

       if stop?
         @logger.info("stop while attempting to read log file")
         break
       end
       # 3. obj 转化
       getObject(key) { |log|

         # 4. codec 并发送消息
         @codec.decode(log) do |event|
           decorate(event)
           queue << event
         end
       }

       # 5. 记录 marker
       @markerConfig.setMarker(key)
       @logger.info('Marker end: ' + @markerConfig.getMarker)
    end
  end


  # 获取下载输入流
  def getObject(key, &block)
    getObjectRequest = GetObjectRequest.new(@bucket, key)
    ks3Object = @ks3client.getObject(getObjectRequest).getObject()
    ks3ObjectInput = ks3Object.getObjectContent()
    buffered =BufferedReader.new(InputStreamReader.new(ks3ObjectInput))
    while (line = buffered.readLine())
      block.call(line)
    end
  end


  # logstash 关闭回调
  def stop
    @markerConfig.ensureMarker
    @logger.info('Stop ks3 input!')
    @logger.info('Marker record: ' + @markerConfig.getMarker)
    Stud.stop!(@current_thread)
  end
end # class LogStash::Inputs::ks3


# 标记配置工具
class MarkerConfig
  KEY_MARKER = 'next_marker'

  def initialize(filename)
    @filename = filename
    dirname = File.dirname(@filename)
    unless Dir.exist?(dirname)
      FileUtils.mkdir_p(dirname)
    end

    if File.exists?(@filename)
      @config = YAML.load_file(@filename)
    else
      @config = {KEY_MARKER => nil}
        File.open(@filename, 'w') do |handler|
          handler.write @config.to_yaml
        end
      end
    end

  def getMarker
    @config[KEY_MARKER] || ''
  end

  public
  def setMarker (marker)
    @config[KEY_MARKER] = marker
  end

  public
  def ensureMarker
    File.open(@filename, 'w') do |handler|
      handler.write @config.to_yaml
    end
  end

end # class bucket 读取配置
