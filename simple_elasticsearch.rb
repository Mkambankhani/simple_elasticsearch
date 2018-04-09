require 'rails'
require 'yaml'
module SimpleElasticsearch
	SETTING = YAML.load_file("#{Rails.root}/config/elasticsearch.yml")['elasticsearch']
	def self.connect?
		if self.query_connection.blank? && self.query_connection["name"].blank?
			return false 
		else
			return true
		end
	end

	def self.query_connection
		begin
			connection = "curl http://#{SETTING['host']}:#{SETTING['port']}"
			return JSON.parse(`#{connection}`)
		rescue Exception => e
			return {}
		end

	end

	def self.init
		config_dir = "#{Rails.root}/config/elasticsearch.yml.example"
		File.open(config_dir, "w+") do |f|
		  f.write("elasticsearch:")
		  f.write("\n index: test_index")
		  f.write("\n type: document")
		  f.write("\n port: 9200")
		  f.write("\n host: localhost")
		  f.write("\n precision: 100")
		end
		config_dir = "#{Rails.root}/config/elasticsearch.yml"
		File.open(config_dir, "w+") do |f|
		  f.write("elasticsearch:")
		  f.write("\n index: test_index")
		  f.write("\n type: document")
		  f.write("\n port: 9200")
		  f.write("\n host: localhost")
		  f.write("\n precision: 100")
		end
	end

	def self.create(record)
		create_string = self.escape_single_quotes(record.as_json.to_json)
		counter = self.count + 1
		create_query = "curl -XPUT 'http://#{SETTING['host']}:#{SETTING['port']}/#{SETTING['index']}/#{SETTING['type']}/#{counter}'  -d '
								#{create_string}'"
		`#{create_query}`							
		return self.find(counter)
	end

	#Retriving record from elastic research
	def self.find(id)
		find_query = "curl -XGET 'http://#{SETTING['host']}:#{SETTING['port']}/#{SETTING['index']}/#{SETTING['type']}/#{id}' "
		begin
			record = JSON.parse(`#{find_query}`)
			return record["_source"].merge({"id" => record["_id"]})
		rescue Exception => e
			return {}
		end
	end
	
	def self.all(type="")
		find_all = "curl -XGET 'http://#{SETTING['host']}:#{SETTING['port']}/#{SETTING['index']}/#{type.present? ? type : SETTING['type']}/_search?pretty=true'"
		return JSON.parse(`#{find_all}`)["hits"]["hits"].collect{|hit| hit["_source"].merge({"id" => hit["_id"]})}
	end

	def self.must_match_by(params)
		params_keys = params.keys rescue nil
		if params_keys.present?

			match = []
			params_keys.each do |key|
				match << {:match => { key => params[key]}}
			end
			query = "curl -XGET 'http://#{SETTING['host']}:#{SETTING['port']}/#{SETTING['index']}/#{SETTING['type']}/_search?pretty=true'  -d '
              {
               	\"query\": {
                  \"bool\": {
                    \"must\":#{self.escape_single_quotes(match.to_json.to_s)}
                    }
                  }
                }
              }'"
            return JSON.parse(`#{query}`)["hits"]["hits"].collect{|hit| hit["_source"].merge({"id" => hit["_id"]})}
		else
			return "parameter bad format"
		end
	end

	def self.should_match_by(params)
		params_keys = params.keys rescue nil
		if params_keys.present?

			match = []
			params_keys.each do |key|
				match << {:match => { key => params[key]}}
			end
			query = "curl -XGET 'http://#{SETTING['host']}:#{SETTING['port']}/#{SETTING['index']}/#{SETTING['type']}/_search?pretty=true'  -d '
              {
               	\"query\": {
                  \"bool\": {
                    \"should\":#{self.escape_single_quotes(match.to_json.to_s)}
                    }
                  }
                }
              }'"
            return JSON.parse(`#{query}`)["hits"]["hits"].collect{|hit| hit["_source"].merge({"id" => hit["_id"]})}
		else
			return "parameter bad format"
		end
	end

	def self.match_all(query_string)
		query = "curl -XGET 'http://#{SETTING['host']}:#{SETTING['port']}/#{SETTING['index']}/#{SETTING['type']}/_search?pretty=true'  -d '
              {
               	\"query\": {
                  \"match\": {
                    \"_all\":\"#{query_string}\"
                    }
                  }
                }
        }'"
        return JSON.parse(`#{query}`)["hits"]["hits"].collect{|hit| hit["_source"].merge({"id" => hit["_id"]})}
	end

	def self.match_by_query(query)
		query = "curl -XGET 'http://#{SETTING['host']}:#{SETTING['port']}/#{SETTING['index']}/#{SETTING['type']}/_search?pretty=true'  -d '
            	#{query.to_json.to_s}'"
        puts query
        return JSON.parse(`#{query}`)["hits"]["hits"].collect{|hit| hit["_source"].merge({"id" => hit["_id"]})}		
	end

	#Delete a document from elastic search
	def self.delete(id)
		delete_query = "curl -XDELETE 'http://#{SETTING['host']}:#{SETTING['port']}/#{SETTING['index']}/#{SETTING['type']}/#{id}'"
		return JSON.parse(`#{delete_query}`)["found"]
	end

	#Update to elastic search
	def self.update(id,updates)
		document = self.find(id)
		if document.blank?
			puts "Document doesn't exist"
			return false
		else
			content = document
			content = self.escape_single_quotes(content.merge(updates).as_json.to_json)
			update_query = "curl -XPUT 'http://#{SETTING['host']}:#{SETTING['port']}/#{SETTING['index']}/#{SETTING['type']}/#{id}'  -d '
								#{content}'"
			return JSON.parse(`#{update_query}`)["result"] == "updated" ? self.find(id) : false
		end
	end

	def self.count
		count_query = "curl -XGET 'http://#{SETTING['host']}:#{SETTING['port']}/#{SETTING['index']}/#{SETTING['type']}/_search?pretty=true'"
		return JSON.parse(`#{count_query}`)["hits"]["total"] rescue 0
	end
	
	def self.escape_single_quotes(string)
	    if string.present?
	        string = string.gsub("'", "'\\\\''")
	    end
	    return string
	end

end