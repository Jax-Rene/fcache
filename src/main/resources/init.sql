CREATE DATABASE`fcache` /* !40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci  */ /* !80016 DEFAULT ENCRYPTION='N'  */;
CREATE TABLE `cache_task` (
  `name` varchar(128) COLLATE utf8mb4_general_ci NOT NULL COMMENT '缓存名称',
  `type` varchar(16) COLLATE utf8mb4_general_ci NOT NULL COMMENT '缓存类型 - [sql]',
  `content` text COLLATE utf8mb4_general_ci NOT NULL COMMENT '缓存任务执行代码',
  `expired` int(11) DEFAULT '-1' COMMENT '过期时间戳',
  `ttl` int(11) DEFAULT '60' COMMENT '缓存过期时间（秒）'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
