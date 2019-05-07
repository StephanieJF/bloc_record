require 'sqlite3'

module Selection

	def find(*ids)
		unless ids > 0 || ids.is_a?(Integer)
			puts "Error: id must be an integer greater than 0"
		end

  	if ids.length == 1
    	find_one(ids.first)
   	else
     	rows = connection.execute <<-SQL
       	SELECT #{columns.join ","} FROM #{table}
       	WHERE id IN (#{ids.join(",")});
     	SQL

    	rows_to_array(rows)
  	end
  end


	def find_one(id)
		unless id > 0 || id.is_a?(Integer)
			"Error: id must be an integer greater than 0"
			return
		end

		row = connection.get_first_row <<-SQL
			SELECT #{columns.join ","} FROM #{table}
			WHERE id = #{id};
		SQL

		init_object_from_row(row)
	end

	def find_by(attribute, value)
    rows = connection.execute <<-SQL
      SELECT #{columns.join ","} FROM #{table}
      WHERE #{attribute} = #{BlocRecord::Utility.sql_strings(value)};
    SQL

    rows_to_array(rows)
  end

	def take(num=1)
		if num.is_a?(integer) && num >0
			if num > 1
      rows = connection.execute <<-SQL
        SELECT #{columns.join ","} FROM #{table}
        ORDER BY random()
        LIMIT #{num};
      SQL

      rows_to_array(rows)
    	else
      	take_one
    	end
		else
			puts "You must enter a number greater than 0"
		end
  end

	def take_one
    row = connection.get_first_row <<-SQL
      SELECT #{columns.join ","} FROM #{table}
      ORDER BY random()
      LIMIT 1;
    SQL

    init_object_from_row(row)
   end

	def first
    row = connection.get_first_row <<-SQL
      SELECT #{columns.join ","} FROM #{table}
      ORDER BY id ASC LIMIT 1;
    SQL

    init_object_from_row(row)
  end

  def last
    row = connection.get_first_row <<-SQL
      SELECT #{columns.join ","} FROM #{table}
      ORDER BY id DESC LIMIT 1;
    SQL

    init_object_from_row(row)
  end

	def all
    rows = connection.execute <<-SQL
      SELECT #{columns.join ","} FROM #{table};
    SQL

    rows_to_array(rows)
  end

	def method_missing(m, *args, &block)
		if m.to_s =~ /^find_by_(.*)$/
			find_by_($1.to_sym => *args.first)
		else
			super
		end
	end

	private

	def init_object_from_row(row)
		if row
			data = Hash[columns.zip(row)]
			new(data)
		end
	end

	def rows_to_array(rows)
     rows.map { |row| new(Hash[columns.zip(row)]) }
  end
end
