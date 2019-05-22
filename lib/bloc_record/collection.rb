module BlocRecord
	class Collection < Array
		def update_all(updates)
			ids = self.map(&:id)
			self.any? ? self.first.class.update(ids, updates) : false
		end

		def take
			self.class.take_one
		end

		def where(args)
			ids = self.map(&:id)
			self.first.class.where(args)
		end

		def not(args)
			ids = self.map(&:id)
			self.first.class.where.not(args)
		end
	end
end
