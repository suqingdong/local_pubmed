DO $$
DECLARE
    idx RECORD;
    v_start TIMESTAMPTZ;
    v_total_start TIMESTAMPTZ := clock_timestamp();
    v_blocks BIGINT;
BEGIN
    -- 1. 确保扩展已安装
    CREATE EXTENSION IF NOT EXISTS pg_prewarm;
    
    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE '🔥 开始专项预热：2021-2025 年份区间';
    RAISE NOTICE '------------------------------------------------';

    -- 2. 遍历 2021_2025 分区下的所有向量索引和全文索引
    FOR idx IN 
        SELECT 
            schemaname || '.' || indexname AS full_idx,
            indexname
        FROM pg_indexes 
        WHERE tablename LIKE 'p_2021_2025%'  -- 只针对这个特定的一级分区
          AND (indexname LIKE '%vec%' OR indexname LIKE '%ts_en%')
    LOOP
        v_start := clock_timestamp();
        RAISE NOTICE '📦 正在搬运索引: %', idx.indexname;

        -- 执行预热
        SELECT pg_prewarm(idx.full_idx::regclass) INTO v_blocks;
        
        RAISE NOTICE '   ✅ 完成! 加载块数: % | 耗时: %', 
            v_blocks, (clock_timestamp() - v_start);
    END LOOP;

    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE '✨ 2021-2025 预热完毕！总耗时: %', (clock_timestamp() - v_total_start);
    RAISE NOTICE '------------------------------------------------';
END $$;
