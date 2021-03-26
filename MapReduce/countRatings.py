class countRatings(MRJob):                                                                                                                                          
    def steps(self):                                                                                                                                                    
        return [                                                                                                                                                        
            MRStep(mapper=self.mapper_get_ratings,                                                                                                                      
                   reducer=self.reducer_count_ratings)                                                                                                                  
        ]                                                                                                                                                               

    #MAP input to rating,1                                                                                                                                                                   
    def mapper_get_ratings(self, _, line):                                                                                                                              
        (userID, movieID, rating, timestamp) = line.split('\t')                                                                                                         
        yield rating, 1                                                                                                                                                 

    #REDUCE each ratings and sum all the 1.                                                                                                                                                                     
    def reducer_count_ratings(self, key, values):                                                                                                                       
        yield key, sum(values)                                                                                                                                          
                                                                                                                                                                        
if __name__ == '__main__':                                                                                                                                              
    countRatings.run()    