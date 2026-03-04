DO $$
DECLARE
    r RECORD;
    v_blocks BIGINT;
    -- 💡 匹配所有 2021-2025 的子表以及未来的子表
    v_pattern TEXT := '^(p_2021_2025_h|p_future_h)'; 
BEGIN
    CREATE EXTENSION IF NOT EXISTS pg_prewarm;

    RAISE NOTICE '🚀 开始全链路预热 (范围: 2021-2025 + Future 2026+)...';

    -- 1. 预热主表 (Heap)
    FOR r IN 
        SELECT oid, relname 
        FROM pg_class 
        WHERE relname ~ v_pattern 
          AND relkind = 'r' 
          AND pg_relation_size(oid) > 0 
    LOOP
        BEGIN
            PERFORM pg_prewarm(r.oid);
            RAISE NOTICE '✅ 已加载表: % (%)', r.relname, pg_size_pretty(pg_relation_size(r.oid));
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING '⚠️ 跳过表 %: %', r.relname, SQLERRM;
        END;
    END LOOP;

    -- 2. 预热 TOAST 表 (摘要原文)
    FOR r IN 
        SELECT reltoastrelid as oid, relname as parent_name
        FROM pg_class 
        WHERE relname ~ v_pattern 
          AND relkind = 'r' 
          AND reltoastrelid != 0
          AND pg_relation_size(reltoastrelid) > 0
    LOOP
        BEGIN
            PERFORM pg_prewarm(r.oid);
            RAISE NOTICE '✅ 已加载 TOAST: 来自 %', r.parent_name;
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING '⚠️ 跳过 TOAST %', r.oid;
        END;
    END LOOP;

    -- 3. 预热所有相关索引 (Vector, GIN, B-tree)
    FOR r IN 
        SELECT i.indexrelid as oid, c_idx.relname as idxname, c_parent.relname as table_name
        FROM pg_index i
        JOIN pg_class c_parent ON c_parent.oid = i.indrelid
        JOIN pg_class c_idx ON c_idx.oid = i.indexrelid
        WHERE c_parent.relname ~ v_pattern
          AND pg_relation_size(i.indexrelid) > 0 
    LOOP
        BEGIN
            PERFORM pg_prewarm(r.oid);
            RAISE NOTICE '✅ 已加载索引: % (属于 %)', r.idxname, r.table_name;
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING '⚠️ 跳过索引 %: %', r.idxname, SQLERRM;
        END;
    END LOOP;

    RAISE NOTICE '✨ 2021-2026+ 全链路预热完成！';
END $$;
