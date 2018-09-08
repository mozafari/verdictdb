class ResultSet:

	rowcount = 0
	description = []

	def __init__(self, resultset):
		self.set = resultset
		self.columncount = self.set.getColumnCount()
		# get .description attribute of resultset
		for i in range(self.columncount):
			info = {}
			info['name'] = self.set.getColumnName(i)
			info['type_code'] = self.set.getColumnType(i)
			info['display_size'] = None # TODO
			info['internal_size'] = None # TODO
			info['precision'] = None # TODO
			info['scale'] = None # TODO
			info['null_ok'] = None # TODO
			self.description.append(info)

		self.rowcount = self.set.getRowCount()


	def fetchone(self):
		if self.set.next():
			row = []
			for i in range(self.columncount):
				value = self.set.getValue(i)
				col_type = self.description[i]['type_code']
				if value is None:
					row.append(None)
				elif col_type in [6, 7, 8]: # FLOAT/REAL/DOUBLE
					row.append(float(value))
				else:
					row.append(value)
			return row
		return None
