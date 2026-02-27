DO $$
DECLARE
    r RECORD;
    v_blocks BIGINT;
BEGIN
    CREATE EXTENSION IF NOT EXISTS pg_prewarm;

    RAISE NOTICE '🚀 开始全链路安全预热 (已过滤空对象)...';

    -- 1. 预热主表 (仅限有物理文件的表)
    FOR r IN 
        SELECT oid, relname 
        FROM pg_class 
        WHERE relname ~ 'p_2021_2025' 
          AND relkind = 'r' 
          AND pg_relation_size(oid) > 0 
    LOOP
        BEGIN
            PERFORM pg_prewarm(r.oid);
            RAISE NOTICE '✅ 已加载表: %', r.relname;
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING '⚠️ 跳过表 %: %', r.relname, SQLERRM;
        END;
    END LOOP;

    -- 2. 预热 TOAST 表 (存储摘要原文)
    FOR r IN 
        SELECT reltoastrelid as oid
        FROM pg_class 
        WHERE relname ~ 'p_2021_2025' 
          AND relkind = 'r' 
          AND reltoastrelid != 0
          AND pg_relation_size(reltoastrelid) > 0
    LOOP
        BEGIN
            PERFORM pg_prewarm(r.oid);
            RAISE NOTICE '✅ 已加载 TOAST: %', r.oid::regclass;
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING '⚠️ 跳过 TOAST %', r.oid;
        END;
    END LOOP;

    -- 3. 预热索引 (关键：过滤掉没有 main fork 的索引)
    FOR r IN 
        SELECT i.indexrelid as oid, c_idx.relname as idxname
        FROM pg_index i
        JOIN pg_class c_parent ON c_parent.oid = i.indrelid
        JOIN pg_class c_idx ON c_idx.oid = i.indexrelid
        WHERE c_parent.relname ~ 'p_2021_2025'
          AND pg_relation_size(i.indexrelid) > 0 -- 必须有物理尺寸
    LOOP
        BEGIN
            PERFORM pg_prewarm(r.oid);
            RAISE NOTICE '✅ 已加载索引: %', r.idxname;
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING '⚠️ 跳过索引 %: %', r.idxname, SQLERRM;
        END;
    END LOOP;

    RAISE NOTICE '✨ 2021-2025 全链路预热完成！';
END $$;