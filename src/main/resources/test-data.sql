INSERT INTO fcache.`cache_task` (`name`, `type`, `content`, `ttl`, `database`) VALUES ('user', 'sql', 'SELECT name,age,score from mydata.`user`', '60', 'mydb');
INSERT INTO fcache.`source_db_info` (`name`, `db_info`) VALUES ('mydb', '{\n    \"jdbc.mysql.url\" : \"jdbc:mysql://127.0.0.1:3306/mydata\",\n    \"jdbc.mysql.username\" : \"root\",\n    \"jdbc.mysql.password\" : \"123456\"\n}');

CREATE DATABASE `mydata` /* !40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci  */ /* !80016 DEFAULT ENCRYPTION='N'  */;
CREATE TABLE mydata.`user` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(128) DEFAULT NULL,
  `age` int(11) DEFAULT NULL,
  `score` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `id` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=utf8mb4;
INSERT INTO mydata.`user` (`id`, `name`, `age`, `score`) VALUES ('1', 'zhangsan', '10', '99'),
('2', 'lisu', '11', '78'),
('3', 'wangwu', '11', '88'),
('4', 'zhuangsan', '15', '88'),
('5', 'liliu', '11', '88');
