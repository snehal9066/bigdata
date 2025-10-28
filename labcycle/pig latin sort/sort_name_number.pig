-- Load name and number
data = LOAD '/home/cusat/Desktop/input.txt' USING PigStorage(' ') AS (name:chararray, num:int);

-- Sort by number ascending
sorted_data = ORDER data BY num ASC;

-- Show output on terminal
DUMP sorted_data;

