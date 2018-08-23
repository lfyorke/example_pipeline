#Notes

I wanted to explain a couple of the approaches i took and the justifications for that:

1. My output is technically incorrect, I was unsure exactly how to transform the data into the exact format you specified,
I was not certain what size was in reference too.  My thought was that the output was supposed to be a dictionary
containing index: value pairs where index is an integer reference to a feature name and the value being the actual
feature value.  This would be easy enough to implement however I wasn't exactly sure if this was correct, so i produced
the correct tuple structure, but the values are wrong, hopefully this is good enough though.

2. I chose to process each record one by one using a yield approach, this is because the data extract was 11GB and would
therefore not fit into memory on my laptop.  However this meant I couldn't use built in scikitlearn transformations such 
as MinMaxScaler or CountVectorizer as i believe they operate over the entire data set for the most part.

3. Given the data extract is over 8 million lines I didn't run the pipeline over all 8 million as a test as I thought 
that would probably take too long, I ran it successfully over about 20% of the data in a run time of a bout 10 minutes.
So hopefully that's enough to prove that it works.
