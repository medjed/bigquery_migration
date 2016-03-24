class BigqueryMigration
  class HashUtil
    def self.deep_symbolize_keys(hash)
      if hash.is_a?(Hash)
        hash.map do |key, val|
          new_key = key.to_sym
          new_val = deep_symbolize_keys(val)
          [new_key, new_val]
        end.to_h
      elsif hash.is_a?(Array)
        hash.map do |val|
          deep_symbolize_keys(val)
        end
      else
        hash
      end
    end

    def self.deep_stringify_keys(hash)
      if hash.is_a?(Hash)
        hash.map do |key, val|
          new_key = key.to_s
          new_val = deep_stringify_keys(val)
          [new_key, new_val]
        end.to_h
      elsif hash.is_a?(Array)
        hash.map do |val|
          deep_stringify_keys(val)
        end
      else
        hash
      end
    end
  end
end
