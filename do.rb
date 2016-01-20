require 'pathname'
require 'logger'
require 'fileutils'
require 'ostruct'
require 'json'

if ARGV.size != 2
	puts "ArgumentError: wrong number of arguments(#{ARGV.size} for 2)"
	exit 1
end
$file_path = ARGV[0]
$result_file_name = ARGV[1]
$current_path =  Dir::pwd
$home_path  = Pathname.new(File.dirname(__FILE__)).realpath

log_formatter = proc{|severity, datatime,progname,msg|
	"#{msg}\n"
}

$bad = Logger.new("#{$home_path}/#{$result_file_name}.bad")
$bad.level = Logger::INFO
$bad.formatter = log_formatter

$result = Logger.new("#{$current_path}/#{$result_file_name}")
$result.level = Logger::INFO
$result.formatter = log_formatter

$log = Logger.new("#{$home_path}/#{$result_file_name}.daemon")
$log.level = Logger::INFO

module ParseHelper
	def self.regular_adaptor attr_val
		/(^\d+\.\d+)/.match(attr_val)[0]
	end

	def self.bool_adaptor attr_val
		case attr_val
		when "true" then 1
		when "false" then 0
		else 3
		end
	end

	def self.array_adaptor attr_val
		attr_val.is_a?(Array) ? attr_val.join(',') : attr_val
	end

	def self.time_adaptor str
		str.length.eql?(19) ? "#{str[0,4]}-#{str[4,2]}-#{str[6,2]} #{str[8,2]}:#{str[10,2]}:#{str[12,2]}" : nil
	end

	def self.loader_version_adaptor str
		if str =~ /^(\d+)\.(\d+)\./
			"#{$1}.#{$2}"
		else
			nil
		end
	end

	OUTPUT_ATTRS = ["id","uuid","request_time","operator_id","province_id","tag","sub_tag","loader_version","created_at","updated_at","loader_version_short","imei","imsi","imsi1","imsi2","ch"]

	PARAMS_ATTRS = {
		"id" => {
			"log_attr" => "id"	
		},
		"uuid" => {
			"log_attr" => "uuid"
		},
		"request_time" => {
			"log_attr" => "request_time"
		},
		"operator_id" => {
			"log_attr" => "operator_id"
		},
		"province_id" => {
			"log_attr" => "province_id"
		},
		"tag" => {
			"log_attr" => "tag"
		},
		"sub_tag" => {
			"log_attr" => "sub_tag"
		},
		"loader_version" => {
			"log_attr" => "loader_version"
		},
		"created_at" => {
			"log_attr" => "created_at"
		},
		"updated_at" => {
			"log_attr" => "updated_at"
		},
		"loader_version_short" => {
			"log_attr" => "loader_version",
			"adaptor" => self.method("loader_version_adaptor")
		},
		"imei" => {
			"log_attr" => "imei"
		},
		"imsi" => {
			"log_attr" => "imsi"
		},
		"imsi1" => {
			"log_attr" => "imsi1"
		},
		"imsi2" => {
			"log_attr" => "imsi1"
		},
		"ch" => {
			"log_attr" => "ch"
		}
	}

	# PARSE_RULES = {
	# 	"user" => {
	# 		"data_struct" => OpenStruct.new(user_attrs),
	# 		"attrs_map" => {
	# 		}
	# 	},
	# 	"location" => {
	# 		"data_struct" => OpenStruct.new(location_attrs),
	# 		"attrs_map" => {
	# 		}
	# 	},
	# 	"params" => {
	# 		"data_struct" => OpenStruct.new(PARAMS_ATTRS.keys),
	# 		"attrs_map" => PARAMS_ATTRS
	# 	}
	# }
end

class LogParser
	include ParseHelper

	def run
		File.open("#{$file_path}").each_line do |line|
			begin
				message_body = line.slice(/\{.*\}/)
				# hostname = line.slice(/(?<=hostname\:)\S+/)
				message = JSON.parse(message_body)
				params = message["params"]
				params["uuid"] = message["user"]["current_uuid"]
				next if params["uuid"] == "600000001"
				params["request_time"] = line[8,19]
				params["operator_id"] = message["location"]["operatorId"]
				params["province_id"] = message["location"]["provinceId"]
				params["id"] = 2426448792
				params["created_at"] = Time.now.strftime("%F %T")
				params["updated_at"] = Time.now.strftime("%F %T")

				data_struct = OpenStruct.new(PARAMS_ATTRS.keys)

				PARAMS_ATTRS.each do |col, opt|
					attr_val = opt["adaptor"].nil? ? params[opt["log_attr"]] : opt["adaptor"].call(params[opt["log_attr"]])
					data_struct.send("#{col}=", attr_val)
				end
				$result.info OUTPUT_ATTRS.collect{|attr| data_struct.method(attr).call}.join('|')

			rescue Exception => err
				$bad.info line
				$log.error err
				$log.error err.message
				$log.error err.backtrace.join("\n")
				next
			end
		end
	end
end

LogParser.new.run