require 'sqlite3'
require 'pg'

module Connection
	def connection(database)
	  @connection ||= database == 'sqlite3' ? sql_database : pg_database
	end

	private

	def sql_database
		SQLite3::Database.new(BlocRecord.database_filename)
	end

	def pg_database
		PG::Database.new(BlocRecord.database_filename)
	end
end
