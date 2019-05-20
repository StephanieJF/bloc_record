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
    FROM #{table}
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

	def where(*args)
		if args.count > 1
			expression = args.shift
			params = args
		else
			case args.first
			when String
				expression = args.first
			when Hash
				expression_hash = BlocRecord::Utility.convert_keys(args.first)
				expression = expression_hash.map {|key, value| "#{key}=#{BlocRecord::Utility.sql_strings(value)}"}.join(" and ")
			end
		end

		sql = <<-SQL
			SELECT #{columns.join ","} FROM #{table}
			WHERE #{expression};
		SQL

		rows = connection.execute(sql, params)
		rows_to_array(rows)
	end

	def order(*args)
		args.map! do |arg|
			if arg.class == Hash
				args_hash = BlocRecord::Utility.convert_keys(arg)
				args_hash.map {|key, value| "#{key}" " #{value}"}
			elsif arg.class == Symbol
				arg.to_s
			else
				arg
			end
		end

		order = args.join(', ')
		uppercase =
		{ "asc" => "ASC",
			"desc" => "DESC" }

		order.gsub!(/\w+/) do |word|
			uppercase.fetch(word,word)
		end

		rows = connection.execute <<-SQL
			SELECT * FROM #{table}
			ORDER BY #{order};
		SQL

		rows_to_array(rows)
	end

	def join(*args)
		if args.count > 1
			joins = args.map { |arg| "INNER JOIN #{arg} ON #{arg}.#{table}_id = #{table}.id"}.join(" ")
			rows = connection.execute <<-SQL
				SELECT * FROM #{table} #{joins}
			SQL
		else
			case args.first
			when String
				rows = connection.execute <<-SQL
					SELECT * FROM #{table}
					INNER JOIN #{args.first} ON #{args.first}.#{table}_id = #{table}.id
				SQL
			when Symbol
				rows = connection.execute <<-SQL
					SELECT * FROM #{table}
					INNER JOIN #{args.first} ON #{args.first}.#{table}_id = #{table}.id
				SQL
			when Hash
				arg_hash = BlocRecord::Utility.convert_keys(args.first)
				association_join = arg_hash.map {|key, value| "INNER JOIN #{key} ON #{key}.#{table}_id = #{table}.id
				INNER JOIN #{value} ON #{value}.#{key}_id = #{key}.id"}.join("")
				rows = connection.execute <<-SQL
					SELECT * FROM #{table} #{association_join}
				SQL
			end
		end

		rows_to_array(rows)
	end

  private

  def init_object_from_row(row)
    if row
      data = Hash[columns.zip(row)]
      new(data)
    end
  end

  def rows_to_array(rows)
    collection = BlocRecord::Collection.new
		rows.each { |row| collection << new(Hash[columns.zip(row)]) }
		collection
  end

  def id_validation(id)
    unless id > 0 || id.is_a?(Integer)
      "Error: id must be an integer greater than 0"
      return
    end
	end
end
