CREATE DATABASE`fcache` /* !40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci  */ /* !80016 DEFAULT ENCRYPTION='N'  */;

CREATE TABLE `cache_task` (
  `name` varchar(128) NOT NULL COMMENT '缓存名称',
  `type` varchar(16) NOT NULL COMMENT '缓存类型 - [sql]',
  `content` text NOT NULL COMMENT '缓存任务执行代码',
  `ttl` int(11) DEFAULT '60' COMMENT '缓存过期时间（秒）',
  `database` varchar(32) NOT NULL DEFAULT '60' COMMENT '加载数据库'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `source_db_info` (
  `name` varchar(32) NOT NULL,
  `db_info` text NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

