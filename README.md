# NAS 媒体库管理工具

Forked from [NAStool/nas-tools](https://github.com/NAStool/nas-tools)

项目详情以及使用方式请移步原项目。

Docker：[链接](https://hub.docker.com/repository/docker/n120318/nas-tools)

## 背景

1. 一些自己修改的功能还不够通用和成熟，暂时不会 PR 到原版 nas-tool
2. 一些自认为足够通用的 PR，但被原项目驳回
3. 原项目不再支持 Jackett 了，但我很需要它
4. 相信一些人也会和我一样拥有上面这些困扰

## 当前和原版的功能差异

基于原项目版本：v3.1.5 详见：[功能差异](feature.md)

## 开发计划

1. 由于原项目停止维护，此项目将会独立迭代，但是迭代速度会非常慢。
2. 受限于原项目关闭 Issue，这里也没有 Issue，我暂时也没有想好如何让大家开 Issue。
3. Pull Request 是欢迎的。
4. 会有 bug，但是你如果有办法提出，我会尝试修复。
5. 后续可能会精简一些功能。

## 分支与构建说明

1. `master` 分支会尽可能地从原项目合并 Release tag，会构建 latest 和 tag 镜像，其中 tag 会和原项目版本号保持一致
2. `dev` 分支是我个人的开发分支，其中会存在当前满足我个人使用，但尚未开发完成的功能，会构建 beta 镜像

## 附：和主项目的分支细节

![image](https://user-images.githubusercontent.com/20685540/219598179-238ce668-991b-457f-9f88-152d12590521.png)

## 写在最后

nas-tool 是一个非常实用的项目，对于国内用户，它能解决非常多的问题，使用没有历史包袱的 nas-tool， 你可能不再需要 Sonarr, Radarr, Ombi 等工具。

在此感谢原作者以及提交过 PR 的众多开发者的辛勤付出。nas-tool 还很年轻，如果你也喜欢 nas-tool，且拥有一定的开发能力，还望多多贡献 PR，让它变得更好！
