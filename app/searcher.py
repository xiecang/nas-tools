import log
from app.helper import DbHelper
from app.indexer import Indexer
from config import Config
from app.message import Message
from app.downloader import Downloader
from app.media import Media
from app.helper import ProgressHelper
from app.utils.types import SearchType, MediaType
from app.media.meta import MetaVideo


class Searcher:
    downloader = None
    media = None
    message = None
    indexer = None
    progress = None
    dbhelper = None

    def __init__(self):
        self.downloader = Downloader()
        self.media = Media()
        self.message = Message()
        self.progress = ProgressHelper()
        self.dbhelper = DbHelper()
        self.indexer = Indexer()
        self.init_config()

    def init_config(self):
        pass

    def search_medias(self,
                      key_word: [str, list],
                      filter_args: dict,
                      match_media=None,
                      in_from: SearchType = None):
        """
        根据关键字调用索引器检查媒体
        :param key_word: 检索的关键字，不能为空
        :param filter_args: 过滤条件
        :param match_media: 区配的媒体信息
        :param in_from: 搜索渠道
        :return: 命中的资源媒体信息列表
        """
        if not key_word:
            return []
        if not self.indexer:
            return []
        return self.indexer.search_by_keyword(key_word=key_word,
                                              filter_args=filter_args,
                                              match_media=match_media,
                                              in_from=in_from)

    def search_one_media(self, media_info: MetaVideo,
                         in_from: SearchType,
                         sites: list = None,
                         filters: dict = None):
        """
        只检索和下载一个资源，用于精确检索下载，由微信、Telegram或豆瓣调用
        :param media_info: 已识别的媒体信息
        :param in_from: 搜索渠道
        :param no_exists: 缺失的剧集清单
        :param sites: 检索哪些站点
        :param filters: 过滤条件，为空则不过滤
        :param user_name: 用户名
        :return: 请求的资源是否全部下载完整，如完整则返回媒体信息
                 请求的资源如果是剧集则返回下载后仍然缺失的季集信息
                 搜索到的结果数量
                 下载到的结果数量，如为None则表示未开启自动下载
        """
        if not media_info:
            return None
        # 进度计数重置
        self.progress.start('search')
        # 查找的季
        if media_info.begin_season is None:
            search_season = None
        else:
            search_season = media_info.get_season_list()
        # 查找的集
        search_episode = media_info.get_episode_list()
        if search_episode and not search_season:
            search_season = [1]
        # 过滤条件
        filter_args = {"season": search_season,
                       "episode": search_episode,
                       "year": media_info.year,
                       "type": media_info.type,
                       "site": sites,
                       "seeders": True}
        if filters:
            filter_args.update(filters)
        if media_info.keyword:
            # 直接使用搜索词搜索
            first_search_name = media_info.keyword
            second_search_name = None
        else:
            # 中文名
            if media_info.cn_name:
                search_cn_name = media_info.cn_name
            else:
                search_cn_name = media_info.title
            # 英文名
            search_en_name = None
            if media_info.en_name:
                search_en_name = media_info.en_name
            else:
                if media_info.original_language == "en":
                    search_en_name = media_info.original_title
                else:
                    # 此处使用独立对象，避免影响TMDB语言
                    en_title = Media().get_tmdb_en_title(media_info)
                    if en_title:
                        search_en_name = en_title
            # 两次搜索名称
            second_search_name = None
            if Config().get_config("laboratory").get("search_en_title"):
                if search_en_name:
                    first_search_name = search_en_name
                    second_search_name = search_cn_name
                else:
                    first_search_name = search_cn_name
            else:
                first_search_name = search_cn_name
                if search_en_name:
                    second_search_name = search_en_name
        # 开始搜索
        log.info("【Searcher】开始检索 %s ..." % first_search_name)
        media_list = self.search_medias(key_word=first_search_name,
                                        filter_args=filter_args,
                                        match_media=media_info,
                                        in_from=in_from)
        # 使用名称重新搜索
        if len(media_list) == 0 \
                and second_search_name \
                and second_search_name != first_search_name:
            log.info("【Searcher】%s 未检索到资源,尝试通过 %s 重新检索 ..." % (first_search_name, second_search_name))
            media_list = self.search_medias(key_word=second_search_name,
                                            filter_args=filter_args,
                                            match_media=media_info,
                                            in_from=in_from)
        if len(media_list) == 0:
            log.info("【Searcher】%s 未搜索到任何资源" % second_search_name)
            return None
        if in_from in self.message.get_search_types():
            # 保存搜索记录
            self.dbhelper.delete_all_search_torrents()
            # 搜索结果排序
            media_list = sorted(media_list, key=lambda x: "%s%s%s%s" % (str(x.title).ljust(100, ' '),
                                                                        str(x.res_order).rjust(3, '0'),
                                                                        str(x.site_order).rjust(3, '0'),
                                                                        str(x.seeders).rjust(10, '0')),
                                reverse=True)
            # 插入数据库
            self.dbhelper.insert_search_results(media_list)
        return media_list
