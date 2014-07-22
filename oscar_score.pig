set job.name "query_expansion_socring_job";
SET DEFAULT_PARALLEL '$reduce_num';

clk_item = load '$input' using PigStorage('\t') as (query:chararray, item:chararray, clk:int);

--tf just add the count of the same <query,item>
tf_group = group clk_item by ( query, item);
tf_agg = foreach tf_group generate group.query as query, group.item as item, SUM(clk_item.clk) as tf;

-- filter the sparse click by a threshold
tf_item = filter tf_agg by tf > $filter_thr;

--count the total uniq item number
item_list = foreach tf_item generate item;
item_uniq = DISTINCT item_list;
item_group = group item_uniq ALL;
item_num = foreach item_group generate COUNT(item_uniq) as num;

--idf count the item number for every query
idf_group = group tf_item by query;
idf_item = foreach idf_group generate group as query, flatten(tf_item), COUNT(tf_item) as idf, (int)(item_num.num) as doc_num;

--calculate click weight
click_tf_idf = foreach idf_item generate tf_item::query as query, tf_item::item as item , tf_item::tf as tf , idf, (LOG(1.0+tf_item::tf)*LOG10((float)doc_num/idf)) as wqa , (LOG(1.0+tf_item::tf)*LOG10((float)doc_num/idf)*LOG(1.0+tf_item::tf)*LOG10((float)doc_num/idf)) as wqasqur;

--norm the item weight
item_weight_group = group click_tf_idf by item;
sum_item = foreach item_weight_group generate group as item, SUM(click_tf_idf.wqasqur) as norm_val;
norm_item = foreach sum_item generate item, SQRT(norm_val) as norm_val;

--normalize query-item weight
--join norm weight of item to click_tf_idf
query_item_info = join click_tf_idf by item , norm_item by item;
query_item_filter = filter query_item_info by ( norm_item::norm_val> 0.0001);

--norm query-item score
query_item_weight = foreach query_item_filter generate click_tf_idf::query as query, click_tf_idf::item as item, click_tf_idf::tf as tf, click_tf_idf::idf as idf, click_tf_idf::wqa as wqa, norm_item::norm_val as item_weight, ((float)click_tf_idf::wqa/norm_item::norm_val) as std_score;

--cross join to generate query1-item-query2 score
query_item_score1 = foreach query_item_weight generate query, item, std_score, idf;
query_item_score2 = foreach query_item_weight generate query, item, std_score, idf;
--join itself
query1_item_query2 = join query_item_score1 by item, query_item_score2 by item;
--remove the query1-query1 pair
query1_item_query2_uniq = filter query1_item_query2 by ( query_item_score1::query != query_item_score2::query);

--method 1 : use score add 
query1_item_query2_score = foreach query1_item_query2_uniq generate query_item_score1::query as query1, query_item_score2::query as query2,  query_item_score1::item as item, (query_item_score1::std_score + query_item_score2::std_score) as score;

--method 2 : use cosine score 
--query1_item_query2_score = foreach query1_item_query2_uniq generate query_item_score1::query as query1, query_item_score2::query as query2,  query_item_score1::item as item, (query_item_score1::std_score * query_item_score2::std_score)/(query_item_score1::idf * query_item_score2::idf) as score;

--calculate query1-query2 final score by sum of <query1-query2>{item1,item2....}
query1_query2_socre_group = group query1_item_query2_score by (query1,query2);

--add score
query1_query2_socre = foreach query1_query2_socre_group generate group.query1 as query1, group.query2 as query2, SUM(query1_item_query2_score.score) as score;

query1_query2_socre_filter = filter query1_query2_socre by ((score*100) > ($score_thr*100));

--output the score
store query1_query2_socre_filter into '$output/result' using PigStorage('\t');
--store query_item_weight into '$output/query_item_weight' using  PigStorage('\t');
--store query1_item_query2_score into '$output/query1_item_query2_score' using PigStorage('\t');
--store click_tf_idf into '$output/click_tf_idf' using PigStorage('\t');
--store norm_item into '$output/norm_item' using PigStorage('\t');
