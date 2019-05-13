require 'sqlite3'

module Selection

	def find(*ids)

  	if ids.length == 1
    	find_one(ids.first)
   	else
			ids.each do |id|
				id_validation(id)
			end
     	rows = connection.execute <<-SQL
       	SELECT #{columns.join ","} FROM #{table}
       	WHERE id IN (#{ids.join(",")});
     	SQL

    	rows_to_array(rows)
  	end
  end


	def find_one(id)
		id_validation(id)

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
		id_validation(num)
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
    s = m.split('_')[2, m.length - 1].join("_").to_sym
    find_by(s, args)
  end

#find_each method with optional batch_size support
	def find_each(attribute, value, batch_size=10)
    rows = connection.execute <<-SQL
      SELECT #{columns.join ","} FROM #{table}
      WHERE #{attribute} = #{BlocRecord::Utility.sql_strings(value)}
			LIMIT #{batch_size};
    SQL

		rows.each do |row|
			yield(rows_to_array(row))
		end
  end

	def find_in_batches(offset=0, batch_size=10)
		rows = connection.execute <<-SQL
			SELECT #{columns.join ","}, total=COUNT(*)
			FROM #{table};
			ORDER BY id ASC
			OFFSET #{offset} ROWS
			FETCH NEXT #{batch_size} ROWS ONLY;
		SQL

		yield (rows_to_array(rows))

		offset=offset+batch_size
		remaining=total-offset
		if remaining < 10
			find_in_batches(offset, remaining)
		else
			find_in_batches(offset, batch_size)
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

	def id_validation(id)
		unless id > 0 || id.is_a?(Integer)
			"Error: id must be an integer greater than 0"
			return
		end
	end
