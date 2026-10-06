-- The embedding functions got their own quota settings in 26.10. Before that they shared the text
-- functions' budget, so `compatibility = 26.9` must restore the limits they used to be bound by:
-- `ai_function_max_api_calls_per_query` = 1000 and `ai_function_max_input_tokens_per_query` = 1000000.

SELECT '-- Current defaults';
SELECT getSetting('ai_function_embedding_max_api_calls_per_query'), getSetting('ai_function_embedding_max_input_tokens_per_query');

SELECT '-- compatibility = 26.9 restores the pre-split limits';
SET compatibility = '26.9';
SELECT getSetting('ai_function_embedding_max_api_calls_per_query'), getSetting('ai_function_embedding_max_input_tokens_per_query');

SET compatibility = '';
