   def poke(self, context):

        self.curent_files=set()
        for dirpath,dirname,filenames in os.walk(self.path):
            self.curent_files.update(filenames)
        unseen = self.curent_files.difference(self.accepted)
        for file in unseen:
            if file not in self.condidates:
                file_path = Path(self.path)/Path(file)
                file_size = file_path.stat().st_size
                safe_cnt = 0
                self.condidates[file] = {"size": file_size, "cnt": safe_cnt}
            else:
                file_path = Path(self.path)/Path(file)
                cur_size = file_path.stat().st_size
                prev_size = self.condidates[file]["size"]
                if self.condidates[file]["cnt"] == 2:
                    self.accepted.add(file)

                if prev_size == cur_size:
                    self.condidates[file]["cnt"]+=1
                else:
                    self.condidates[file]["size"] = cur_size
                    self.condidates[file]["cnt"] = 0
            
            if len(self.condidates)==0:
                return False
            else:

        for file, params in self.condidates.items():
            size = params[0]
            cnt = params[-1]
            if cnt==0:
                self.accepted.add(file)
            if cnt is None:
                self.prev_sizes[file] = size
                return False
