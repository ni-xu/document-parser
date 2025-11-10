This project was made by Nicholas Xu for his RAship for Dr. Bobby Harris in the Georgia Tech School of Economics.

It takes the input of a large list of delineated articles in one file, submits basic article information into an SQL
table, then runs each entry through an LLM. The results are then submitted to a new SQL table. A random selection of
articles are checked to prevent hallucination. The result is a flattened version of the given articles in data 
analyzable form. 

This process is asychronously controlled by a semaphore that regulates the number of tasks that are run at a time, in
batch_processor, to avoid rate limits. 

By slightly augmenting regex segmentation rules, gemini prompting, and adding a Gemini API key to your local machine,
this script can be repurposed to process any large text file.
