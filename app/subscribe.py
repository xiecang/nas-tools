import json
from operator import attrgetter
from threading import Lock

import log
from app.downloader import Downloader
from app.filter import Filter
from app.helper import DbHelper, MetaHelper
from app.media import Media, DouBan
from app.media.meta import MetaInfo
from app.message import Message
from app.searcher import Searcher
from app.sites import Sites
from app.indexer import Indexer
from app.utils import Torrent
from app.utils.types import MediaType, SearchType
from web.backend.web_utils import WebUtils
from collections import defaultdict


lock = Lock()


class Subscribe:
    dbhelper = None
    metahelper = None
    searcher = None
    message = None
    media = None
    downloader = None
    sites = None
    douban = None
    filter = None

    def __init__(self):
        self.dbhelper = DbHelper()
        self.metahelper = MetaHelper()
        self.searcher = Searcher()
        self.message = Message()
        self.media = Media()
        self.downloader = Downloader()
        self.sites = Sites()
        self.douban = DouBan()
        self.indexer = Indexer()
        self.filter = Filter()

    def add_rss_subscribe(self, mtype, name, year,
                          keyword=None,
                          season=None,
                          fuzzy_match=False,
                          mediaid=None,
                          rss_sites=None,
                          search_sites=None,
                          over_edition=False,
                          filter_restype=None,
                          filter_pix=None,
                          filter_team=None,
                          filter_rule=None,
                          save_path=None,
                          download_setting=None,
                          total_ep=None,
                          current_ep=None,
                          state="D",
                          rssid=None):
        """
        添加电影、电视剧订阅
        :param mtype: 类型，电影、电视剧、动漫
        :param name: 标题
        :param year: 年份，如要是剧集需要是首播年份
        :param keyword: 自定义搜索词
        :param season: 第几季，数字
        :param fuzzy_match: 是否模糊匹配
        :param mediaid: 媒体ID，DB:/BG:/TMDBID
        :param rss_sites: 订阅站点列表，为空则表示全部站点
        :param search_sites: 搜索站点列表，为空则表示全部站点
        :param over_edition: 是否选版
        :param filter_restype: 质量过滤
        :param filter_pix: 分辨率过滤
        :param filter_team: 制作组/字幕组过滤
        :param filter_rule: 关键字过滤
        :param save_path: 保存路径
        :param download_setting: 下载设置
        :param state: 添加订阅时的状态
        :param rssid: 修改订阅时传入
        :param total_ep: 总集数
        :param current_ep: 开始订阅集数
        :return: 错误码：0代表成功，错误信息
        """
        if not name:
            return -1, "标题或类型有误", None
        year = int(year) if str(year).isdigit() else ""
        rss_sites = rss_sites or []
        search_sites = search_sites or []
        over_edition = 1 if over_edition else 0
        filter_rule = int(filter_rule) if str(filter_rule).isdigit() else None
        total_ep = int(total_ep) if str(total_ep).isdigit() else None
        current_ep = int(current_ep) if str(current_ep).isdigit() else None
        download_setting = int(download_setting) if str(download_setting).replace("-", "").isdigit() else ""
        fuzzy_match = True if fuzzy_match else False
        # 检索媒体信息
        if not fuzzy_match:
            # 根据TMDBID查询，从推荐加订阅的情况
            if mediaid:
                # 根据ID查询
                media_info = WebUtils.get_mediainfo_from_id(mtype=mtype, mediaid=mediaid)
            else:
                # 根据名称和年份查询
                if season:
                    title = "%s %s 第%s季".strip() % (name, year, season)
                else:
                    title = "%s %s".strip() % (name, year)
                media_info = self.media.get_media_info(title=title,
                                                       mtype=mtype,
                                                       strict=True if year else False,
                                                       cache=False)
            # 检查TMDB信息
            if not media_info or not media_info.tmdb_info:
                return 1, "无法TMDB查询到媒体信息", None
            # 添加订阅
            if media_info.type != MediaType.MOVIE:
                # 电视剧
                if season:
                    total_episode = self.media.get_tmdb_season_episodes_num(tv_info=media_info.tmdb_info,
                                                                            season=int(season))
                else:
                    # 查询季及集信息
                    total_seasoninfo = self.media.get_tmdb_tv_seasons(tv_info=media_info.tmdb_info)
                    if not total_seasoninfo:
                        return 2, "获取剧集信息失败", media_info
                    # 按季号降序排序
                    total_seasoninfo = sorted(total_seasoninfo,
                                              key=lambda x: x.get("season_number"),
                                              reverse=True)
                    # 取最新季
                    season = total_seasoninfo[0].get("season_number")
                    total_episode = total_seasoninfo[0].get("episode_count")
                if not total_episode:
                    return 3, "第%s季获取剧集数失败，请确认该季是否存在" % season, media_info
                media_info.begin_season = int(season)
                media_info.total_episodes = total_episode
                if total_ep:
                    total = total_ep
                else:
                    total = media_info.total_episodes
                if current_ep:
                    lack = total - current_ep - 1
                else:
                    lack = total
                if rssid:
                    self.dbhelper.delete_rss_tv(rssid=rssid)
                code = self.dbhelper.insert_rss_tv(media_info=media_info,
                                                   total=total,
                                                   lack=lack,
                                                   state=state,
                                                   rss_sites=rss_sites,
                                                   search_sites=search_sites,
                                                   over_edition=over_edition,
                                                   filter_restype=filter_restype,
                                                   filter_pix=filter_pix,
                                                   filter_team=filter_team,
                                                   filter_rule=filter_rule,
                                                   save_path=save_path,
                                                   download_setting=download_setting,
                                                   total_ep=total_ep,
                                                   current_ep=current_ep,
                                                   fuzzy_match=0,
                                                   desc=media_info.overview,
                                                   note=self.gen_rss_note(media_info),
                                                   keyword=keyword,
                                                   rss_id=rssid)
            else:
                # 电影
                if rssid:
                    self.dbhelper.delete_rss_movie(rssid=rssid)
                code = self.dbhelper.insert_rss_movie(media_info=media_info,
                                                      state=state,
                                                      rss_sites=rss_sites,
                                                      search_sites=search_sites,
                                                      over_edition=over_edition,
                                                      filter_restype=filter_restype,
                                                      filter_pix=filter_pix,
                                                      filter_team=filter_team,
                                                      filter_rule=filter_rule,
                                                      save_path=save_path,
                                                      download_setting=download_setting,
                                                      fuzzy_match=0,
                                                      desc=media_info.overview,
                                                      note=self.gen_rss_note(media_info),
                                                      keyword=keyword)
        else:
            # 模糊匹配
            media_info = MetaInfo(title=name, mtype=mtype)
            media_info.title = name
            media_info.type = mtype
            if season:
                media_info.begin_season = int(season)
            if mtype == MediaType.MOVIE:
                if rssid:
                    self.dbhelper.delete_rss_movie(rssid=rssid)
                code = self.dbhelper.insert_rss_movie(media_info=media_info,
                                                      state="R",
                                                      rss_sites=rss_sites,
                                                      search_sites=search_sites,
                                                      over_edition=over_edition,
                                                      filter_restype=filter_restype,
                                                      filter_pix=filter_pix,
                                                      filter_team=filter_team,
                                                      filter_rule=filter_rule,
                                                      save_path=save_path,
                                                      download_setting=download_setting,
                                                      fuzzy_match=1,
                                                      keyword=keyword)
            else:
                if rssid:
                    self.dbhelper.delete_rss_tv(rssid=rssid)
                code = self.dbhelper.insert_rss_tv(media_info=media_info,
                                                   total=0,
                                                   lack=0,
                                                   state="R",
                                                   rss_sites=rss_sites,
                                                   search_sites=search_sites,
                                                   over_edition=over_edition,
                                                   filter_restype=filter_restype,
                                                   filter_pix=filter_pix,
                                                   filter_team=filter_team,
                                                   filter_rule=filter_rule,
                                                   save_path=save_path,
                                                   download_setting=download_setting,
                                                   fuzzy_match=1,
                                                   keyword=keyword,
                                                   rss_id=rssid)

        if code == 0:
            return code, "添加订阅成功", media_info
        elif code == 9:
            return code, "订阅已存在", media_info
        else:
            return code, "添加订阅失败", media_info

    def finish_rss_subscribe(self, rss_info):
        """
        完成订阅
        :param rssid: 订阅ID
        :param media: 识别的媒体信息，发送消息使用
        """
        if not rss_info:
            return
        type = rss_info.get('type')
        rss_id = rss_info.get('id')
        name = rss_info.get('name')
        year = rss_info.get('year')
        overview = rss_info.get("overview")
        poster = rss_info.get('poster')
        tmdb_id = rss_info.get('tmdbid')
        season = rss_info.get('season') or None

        # 电影订阅
        rtype = "MOV" if type == MediaType.MOVIE else "TV"
        if type == MediaType.MOVIE:
            # 查询电影RSS数据
            rss = self.dbhelper.get_rss_movies(rssid=rss_id)
            if not rss:
                return
            # 登记订阅历史
            self.dbhelper.insert_rss_history(rssid=rss_id,
                                             rtype=rtype,
                                             name=name,
                                             year=year,
                                             tmdbid=tmdb_id,
                                             image=poster,
                                             desc=overview)

            # 删除订阅
            self.dbhelper.delete_rss_movie(rssid=rss_id)

        # 电视剧订阅
        else:
            # 查询电视剧RSS数据

            rss = self.dbhelper.get_rss_tvs(rssid=rss_id)
            if not rss:
                return
            total = rss_info.get('total_ep')
            current_ep = rss_info.get('current_ep')
            # 登记订阅历史
            self.dbhelper.insert_rss_history(rssid=rss_id,
                                             rtype=rtype,
                                             name=name,
                                             year=year,
                                             season=season,
                                             tmdbid=tmdb_id,
                                             image=poster,
                                             desc=overview,
                                             total=total,
                                             start=current_ep)
            # 删除订阅
            self.dbhelper.delete_rss_tv(rssid=rss_id, delete_ep=True)

        # 发送订阅完成的消息
        log.info("【Rss】%s %s %s 订阅完成，删除订阅..." % (
            type.value,
            name,
            season,
        ))
        self.message.send_rss_finished_message(rss_info=rss_info)

    def get_subscribe_movies(self, rid=None, state=None):
        """
        获取电影订阅
        """
        ret_dict = {}
        rss_movies = self.dbhelper.get_rss_movies(rssid=rid, state=state)
        rss_sites_valid = self.sites.get_site_names(rss=True)
        search_sites_valid = self.indexer.get_indexer_names()
        for rss_movie in rss_movies:
            desc = rss_movie.DESC
            note = rss_movie.NOTE
            tmdbid = rss_movie.TMDBID
            rss_sites = json.loads(rss_movie.RSS_SITES) if rss_movie.RSS_SITES else []
            search_sites = json.loads(rss_movie.SEARCH_SITES) if rss_movie.SEARCH_SITES else []
            over_edition = True if rss_movie.OVER_EDITION == 1 else False
            filter_restype = rss_movie.FILTER_RESTYPE
            filter_pix = rss_movie.FILTER_PIX
            filter_team = rss_movie.FILTER_TEAM
            filter_rule = rss_movie.FILTER_RULE
            download_setting = rss_movie.DOWNLOAD_SETTING
            save_path = rss_movie.SAVE_PATH
            fuzzy_match = True if rss_movie.FUZZY_MATCH == 1 else False
            keyword = rss_movie.KEYWORD
            # 兼容旧配置
            if desc and desc.find('{') != -1:
                desc = self.__parse_rss_desc(desc)
                rss_sites = desc.get("rss_sites")
                search_sites = desc.get("search_sites")
                over_edition = True if desc.get("over_edition") == 'Y' else False
                filter_restype = desc.get("restype")
                filter_pix = desc.get("pix")
                filter_team = desc.get("team")
                filter_rule = desc.get("rule")
                download_setting = ""
                save_path = ""
                fuzzy_match = False if tmdbid else True
            if note:
                note_info = self.__parse_rss_desc(note)
            else:
                note_info = {}
            rss_sites = [site for site in rss_sites if site in rss_sites_valid]
            search_sites = [site for site in search_sites if site in search_sites_valid]
            ret_dict[str(rss_movie.ID)] = {
                "id": rss_movie.ID,
                "name": rss_movie.NAME,
                "year": rss_movie.YEAR,
                "tmdbid": rss_movie.TMDBID,
                "image": rss_movie.IMAGE,
                "overview": rss_movie.DESC,
                "rss_sites": rss_sites,
                "search_sites": search_sites,
                "over_edition": over_edition,
                "filter_restype": filter_restype,
                "filter_pix": filter_pix,
                "filter_team": filter_team,
                "filter_rule": filter_rule,
                "filter_order": rss_movie.FILTER_ORDER,
                "save_path": save_path,
                "download_setting": download_setting,
                "fuzzy_match": fuzzy_match,
                "state": rss_movie.STATE,
                "poster": note_info.get("poster"),
                "release_date": note_info.get("release_date"),
                "vote": note_info.get("vote"),
                "keyword": keyword,
                "type": MediaType.MOVIE
            }
        return ret_dict

    def get_subscribe_tvs(self, rid=None, state=None):
        ret_dict = {}
        rss_tvs = self.dbhelper.get_rss_tvs(rssid=rid, state=state)
        rss_sites_valid = self.sites.get_site_names(rss=True)
        search_sites_valid = self.indexer.get_indexer_names()
        for rss_tv in rss_tvs:
            desc = rss_tv.DESC
            note = rss_tv.NOTE
            tmdbid = rss_tv.TMDBID
            rss_sites = json.loads(rss_tv.RSS_SITES) if rss_tv.RSS_SITES else []
            search_sites = json.loads(rss_tv.SEARCH_SITES) if rss_tv.SEARCH_SITES else []
            over_edition = True if rss_tv.OVER_EDITION == 1 else False
            filter_restype = rss_tv.FILTER_RESTYPE
            filter_pix = rss_tv.FILTER_PIX
            filter_team = rss_tv.FILTER_TEAM
            filter_rule = rss_tv.FILTER_RULE
            download_setting = rss_tv.DOWNLOAD_SETTING
            save_path = rss_tv.SAVE_PATH
            total_ep = rss_tv.TOTAL_EP
            current_ep = rss_tv.CURRENT_EP
            fuzzy_match = True if rss_tv.FUZZY_MATCH == 1 else False
            keyword = rss_tv.KEYWORD
            # 兼容旧配置
            if desc and desc.find('{') != -1:
                desc = self.__parse_rss_desc(desc)
                rss_sites = desc.get("rss_sites")
                search_sites = desc.get("search_sites")
                over_edition = True if desc.get("over_edition") == 'Y' else False
                filter_restype = desc.get("restype")
                filter_pix = desc.get("pix")
                filter_team = desc.get("team")
                filter_rule = desc.get("rule")
                save_path = ""
                download_setting = ""
                total_ep = desc.get("total")
                current_ep = desc.get("current")
                fuzzy_match = False if tmdbid else True
            if note:
                note_info = self.__parse_rss_desc(note)
            else:
                note_info = {}
            rss_sites = [site for site in rss_sites if site in rss_sites_valid]
            search_sites = [site for site in search_sites if site in search_sites_valid]
            ret_dict[str(rss_tv.ID)] = {
                "id": rss_tv.ID,
                "name": rss_tv.NAME,
                "year": rss_tv.YEAR,
                "season": rss_tv.SEASON,
                "tmdbid": rss_tv.TMDBID,
                "image": rss_tv.IMAGE,
                "overview": rss_tv.DESC,
                "rss_sites": rss_sites,
                "search_sites": search_sites,
                "over_edition": over_edition,
                "filter_restype": filter_restype,
                "filter_pix": filter_pix,
                "filter_team": filter_team,
                "filter_rule": filter_rule,
                "filter_order": rss_tv.FILTER_ORDER,
                "save_path": save_path,
                "download_setting": download_setting,
                "total": rss_tv.TOTAL,
                "lack": rss_tv.LACK,
                "total_ep": total_ep,
                "current_ep": current_ep,
                "fuzzy_match": fuzzy_match,
                "state": rss_tv.STATE,
                "poster": note_info.get("poster"),
                "release_date": note_info.get("release_date"),
                "vote": note_info.get("vote"),
                "keyword": keyword,
                "type": MediaType.TV,
            }
        return ret_dict

    @ staticmethod
    def __parse_rss_desc(desc):
        """
        解析订阅的JSON字段
        """
        if not desc:
            return {}
        return json.loads(desc) or {}

    @ staticmethod
    def gen_rss_note(media):
        """
        生成订阅的JSON备注信息
        :param media: 媒体信息
        :return: 备注信息
        """
        if not media:
            return {}
        note = {
            "poster": media.get_poster_image(),
            "release_date": media.release_date,
            "vote": media.vote_average
        }
        return json.dumps(note)

    def refresh_rss_metainfo(self):
        """
        定时将豆瓣订阅转换为TMDB的订阅，并更新订阅的TMDB信息
        """
        # 更新电影
        log.info("【Subscribe】开始刷新订阅TMDB信息...")
        rss_movies = self.get_subscribe_movies(state='R')
        for rid, rss_info in rss_movies.items():
            # 跳过模糊匹配的
            if rss_info.get("fuzzy_match"):
                continue
            rssid = rss_info.get("id")
            name = rss_info.get("name")
            year = rss_info.get("year") or ""
            tmdbid = rss_info.get("tmdbid")
            # 更新TMDB信息
            media_info = self.__get_media_info(tmdbid=tmdbid,
                                               name=name,
                                               year=year,
                                               mtype=MediaType.MOVIE,
                                               cache=False)
            if media_info and media_info.tmdb_id and media_info.title != name:
                log.info(f"【Subscribe】检测到TMDB信息变化，更新电影订阅 {name} 为 {media_info.title}")
                # 更新订阅信息
                self.dbhelper.update_rss_movie_tmdb(rid=rssid,
                                                    tmdbid=media_info.tmdb_id,
                                                    title=media_info.title,
                                                    year=media_info.year,
                                                    image=media_info.get_message_image(),
                                                    desc=media_info.overview,
                                                    note=self.gen_rss_note(media_info))
                # 清除TMDB缓存
                self.metahelper.delete_meta_data_by_tmdbid(media_info.tmdb_id)

        # 更新电视剧
        rss_tvs = self.get_subscribe_tvs(state='R')
        for rid, rss_info in rss_tvs.items():
            # 跳过模糊匹配的
            if rss_info.get("fuzzy_match"):
                continue
            rssid = rss_info.get("id")
            name = rss_info.get("name")
            year = rss_info.get("year") or ""
            tmdbid = rss_info.get("tmdbid")
            season = rss_info.get("season") or 1
            total = rss_info.get("total")
            total_ep = rss_info.get("total_ep")
            lack = rss_info.get("lack")
            # 更新TMDB信息
            media_info = self.__get_media_info(tmdbid=tmdbid,
                                               name=name,
                                               year=year,
                                               mtype=MediaType.TV,
                                               cache=False)
            if media_info and media_info.tmdb_id:
                # 获取总集数
                total_episode = self.media.get_tmdb_season_episodes_num(tv_info=media_info.tmdb_info,
                                                                        season=int(str(season).replace("S", "")))
                # 设置总集数的，不更新集数
                if total_ep:
                    total_episode = total_ep
                if total_episode and (name != media_info.title or total != total_episode):
                    # 新的缺失集数
                    lack_episode = total_episode - (total - lack)
                    log.info(
                        f"【Subscribe】检测到TMDB信息变化，更新电视剧订阅 {name} 为 {media_info.title}，总集数为：{total_episode}")
                    # 更新订阅信息
                    self.dbhelper.update_rss_tv_tmdb(rid=rssid,
                                                     tmdbid=media_info.tmdb_id,
                                                     title=media_info.title,
                                                     year=media_info.year,
                                                     total=total_episode,
                                                     lack=lack_episode,
                                                     image=media_info.get_message_image(),
                                                     desc=media_info.overview,
                                                     note=self.gen_rss_note(media_info))
                    # 更新缺失季集
                    self.dbhelper.update_rss_tv_episodes(rid=rssid, episodes=range(total - lack + 1, total + 1))
                    # 清除TMDB缓存
                    self.metahelper.delete_meta_data_by_tmdbid(media_info.tmdb_id)
        log.info("【Subscribe】订阅TMDB信息刷新完成")

    def __get_media_info(self, tmdbid, name, year, mtype, cache=True):
        """
        综合返回媒体信息
        """
        if tmdbid and not str(tmdbid).startswith("DB:"):
            media_info = MetaInfo(title="%s %s".strip() % (name, year))
            tmdb_info = self.media.get_tmdb_info(mtype=mtype, tmdbid=tmdbid)
            media_info.set_tmdb_info(tmdb_info)
        else:
            media_info = self.media.get_media_info(title="%s %s" % (name, year), mtype=mtype, strict=True, cache=cache)
        return media_info

    def subscribe_search_all(self):
        """
        搜索R状态的所有订阅，由定时服务调用
        """
        self.subscribe_search(state="R")

    def subscribe_search(self, state="D"):
        """
        RSS订阅队列中状态的任务处理，先进行存量资源检索，缺失的才标志为RSS状态，由定时服务调用
        """
        try:
            lock.acquire()
            # 处理电影
            self.subscribe_search_movie(state=state)
            # 处理电视剧
            self.subscribe_search_tv(state=state)
        finally:
            lock.release()

    def subscribe_search_movie(self, rssid=None, state='D'):
        """
        检索电影RSS
        :param rssid: 订阅ID，未输入时检索所有状态为D的，输入时检索该ID任何状态的
        :param state: 检索的状态，默认为队列中才检索
        """
        if rssid:
            rss_movies = self.get_subscribe_movies(rid=rssid)
        else:
            rss_movies = self.get_subscribe_movies(state=state)
        if rss_movies:
            log.info("【Subscribe】共有 %s 个电影订阅需要检索" % len(rss_movies))
        for rid, rss_info in rss_movies.items():
            # 跳过模糊匹配的
            if rss_info.get("fuzzy_match"):
                continue
            # 搜索站点范围
            rssid = rss_info.get("id")
            name = rss_info.get("name")
            year = rss_info.get("year") or ""
            tmdbid = rss_info.get("tmdbid")
            over_edition = rss_info.get("over_edition")
            keyword = rss_info.get("keyword")

            # 开始搜索
            self.dbhelper.update_rss_movie_state(rssid=rssid, state='S')
            # 识别
            media_info = self.__get_media_info(tmdbid, name, year, MediaType.MOVIE)
            # 未识别到媒体信息
            if not media_info or not media_info.tmdb_info:
                self.dbhelper.update_rss_movie_state(rssid=rssid, state='R')
                continue
            media_info.set_download_info(download_setting=rss_info.get("download_setting"),
                                         save_path=rss_info.get("save_path"))
            # 自定义搜索词
            media_info.keyword = keyword
            # 非洗版的情况检查是否存在
            if not over_edition:
                # 检查是否存在
                exist_flag, _ = self.get_no_exists(media_info, rss_info)
                # 已经存在
                if exist_flag:
                    log.info("【Subscribe】电影 %s 已存在" % media_info.get_title_string())
                    self.finish_rss_subscribe(rss_info=rss_info)
                    continue
            # 开始检索
            filter_dict = {
                "restype": rss_info.get('filter_restype'),
                "pix": rss_info.get('filter_pix'),
                "team": rss_info.get('filter_team'),
                "rule": rss_info.get('filter_rule'),
                "site": rss_info.get("search_sites")
            }
            search_result = self.searcher.search_one_media(
                media_info=media_info,
                in_from=SearchType.RSS,
                sites=rss_info.get("search_sites"),
                filters=filter_dict)
            self.subscribe_media(rss_info, search_result)

    def subscribe_search_tv(self, rssid=None, state="D"):
        """
        检索电视剧RSS
        :param rssid: 订阅ID，未输入时检索所有状态为D的，输入时检索该ID任何状态的
        :param state: 检索的状态，默认为队列中才检索
        """
        if rssid:
            rss_tvs = self.get_subscribe_tvs(rid=rssid)
        else:
            rss_tvs = self.get_subscribe_tvs(state=state)
        if rss_tvs:
            log.info("【Subscribe】共有 %s 个电视剧订阅需要检索" % len(rss_tvs))
        rss_no_exists = {}
        for rid, rss_info in rss_tvs.items():
            # 跳过模糊匹配的
            if rss_info.get("fuzzy_match"):
                continue
            rssid = rss_info.get("id")
            name = rss_info.get("name")
            year = rss_info.get("year") or ""
            tmdbid = rss_info.get("tmdbid")
            over_edition = rss_info.get("over_edition")
            keyword = rss_info.get("keyword")
            # 开始搜索
            self.dbhelper.update_rss_tv_state(rssid=rssid, state='S')
            # 识别
            media_info = self.__get_media_info(tmdbid, name, year, MediaType.TV)
            # 未识别到媒体信息
            if not media_info or not media_info.tmdb_info:
                self.dbhelper.update_rss_tv_state(rssid=rssid, state='R')
                continue
            # 取下载设置
            media_info.set_download_info(download_setting=rss_info.get("download_setting"),
                                         save_path=rss_info.get("save_path"))
            # 订阅季
            # 从登记薄中获取缺失剧集
            season = 1
            if rss_info.get("season"):
                season = int(str(rss_info.get("season")).replace("S", ""))
            media_info.begin_season = season
            # 订阅ID
            media_info.rssid = rssid
            # 自定义搜索词
            media_info.keyword = keyword
            exist_flag, rss_no_exists = self.get_no_exists(media_info, rss_info, over_edition)
            if not over_edition and exist_flag:
                continue

            # 开始检索
            filter_dict = {
                "restype": rss_info.get('filter_restype'),
                "pix": rss_info.get('filter_pix'),
                "team": rss_info.get('filter_team'),
                "rule": rss_info.get('filter_rule'),
                "site": rss_info.get("search_sites")
            }
            search_result = self.searcher.search_one_media(
                media_info=media_info,
                in_from=SearchType.RSS,
                sites=rss_info.get("search_sites"),
                filters=filter_dict)
            # 更新状态
            self.subscribe_media(rss_info, search_result, rss_no_exists)

    def subscribe_media(self, rss_info, media_list: list, no_exists: dict = None):
        rssid = rss_info.get('id')
        over_edition = rss_info.get('over_edition')
        type = rss_info.get('type')
        tmdb_id = int(rss_info.get('tmdbid'))
        title = rss_info.get('name')
        res_order = rss_info.get('filter_order')
        if not media_list:
            return
        # 择优下载
        download_items = []
        need_tvs = defaultdict(list)
        if type == MediaType.MOVIE:
            if over_edition:
                media_list = [m for m in media_list if m.res_order > res_order]
        else:
            if over_edition:
                over_edition_media_list = [m for m in media_list if any(int(m.res_order) > int(no_exists['episode_filter_orders'][x])
                                                                        for x in m.get_episode_list() if no_exists['episode_filter_orders'].get(x, 0) > 0)]
                over_edition_need_tvs = defaultdict(list)
                over_edition_need_tvs[tmdb_id].append({
                    'episodes': [e for e, o in no_exists['episode_filter_orders'].items() if int(o) > 0],
                    'season': no_exists['season'],
                    'total_episodes': no_exists['total_episodes'],
                })
                print(over_edition_need_tvs)
                download_over_edition_items,  _ = self.downloader.batch_download(in_from=SearchType.RSS,
                                                                                 media_list=over_edition_media_list,
                                                                                 need_tvs=over_edition_need_tvs)
                if download_over_edition_items:
                    download_items += download_over_edition_items
            print(download_items)
            need_tvs[tmdb_id].append({
                'episodes': [e for e in no_exists['episodes']],
                'season': no_exists['season'],
                'total_episodes': no_exists['total_episodes'],
            })

        if not media_list:
            return
        download_lacked_items, need_tvs = self.downloader.batch_download(in_from=SearchType.RSS,
                                                                         media_list=media_list,
                                                                         need_tvs=need_tvs)
        download_items += download_lacked_items
        print(download_items)
        if not download_items:
            log.info("【Subscribe】%s 未下载到资源" % title)
        else:
            log.info("【Subscribe】实际下载了 %s 个资源" % len(download_items))
            if type == MediaType.TV:
                for item in download_items:
                    if item.get('season'):
                        no_exists['episode_filter_orders'].update({e: item.get('item').res_order for e in no_exists['episode_filter_orders']})
                        no_exists['episodes'] = []
                    else:
                        no_exists['episode_filter_orders'].update({e: item.get('item').res_order for e in item.get('episodes')})
                        no_exists['episodes'] = list(set(no_exists['episodes']).difference(set(item.get('episodes'))))
                self.update_subscribe_tv_lack(rss_info=rss_info,
                                              seasoninfo=no_exists)
            if type == MediaType.MOVIE or not no_exists['episodes']:
                # 洗版
                if over_edition:
                    if self.update_subscribe_over_edition(rss_info=rss_info,
                                                          media=max([i['item'] for i in download_items], key=attrgetter('res_order'))):
                        self.finish_rss_subscribe(rss_info=rss_info)
                        return
                else:
                    self.finish_rss_subscribe(rss_info=rss_info)
                    return
        self.update_rss_state(type, rssid, 'R')

    def update_rss_state(self, rtype, rssid, state):
        """
        根据类型更新订阅状态
        :param rtype: 订阅类型
        :param rssid: 订阅ID
        :param state: 状态 R/D/S
        """
        if rtype == MediaType.MOVIE:
            self.dbhelper.update_rss_movie_state(rssid=rssid, state=state)
        else:
            self.dbhelper.update_rss_tv_state(rssid=rssid, state=state)

    def update_subscribe_over_edition(self, rss_info, media):
        """
        更新洗版订阅
        :param rtype: 订阅类型
        :param rssid: 订阅ID
        :param media: 含订阅信息的媒体信息
        :return 完成订阅返回True，否则返回False
        """
        rssid = rss_info.get('id')
        if not rssid \
                or not media.res_order \
                or not media.filter_rule \
                or not media.res_order:
            return False
        # 更新订阅命中的优先级
        self.dbhelper.update_rss_filter_order(rtype=media.type,
                                              rssid=rssid,
                                              res_order=media.res_order)
        # 检查是否匹配最高优先级规则
        over_edition_order = self.filter.get_rule_first_order(rulegroup=rss_info.get('filter_rule'))
        if int(media.res_order) >= int(over_edition_order):
            log.info(f"【Subscribe】{rss_info.get('name')}{rss_info.get('year')} {rss_info.get('season') or ''} 洗版完成，新版本优先级Index：{media.res_order}，旧版本优先级Index：{over_edition_order}")
            return True
        return False

    def check_subscribe_over_edition(self, rtype, rssid, res_order):
        """
        检查洗版订阅的优先级
        :param rtype: 订阅类型
        :param rssid: 订阅ID
        :param res_order: 优先级
        :return 资源更优先返回True，否则返回False
        """
        pre_res_order = self.dbhelper.get_rss_overedition_order(rtype=rtype, rssid=rssid)
        if not pre_res_order:
            return True
        return True if int(pre_res_order) < int(res_order) else False

    def update_subscribe_tv_lack(self, rss_info, seasoninfo):
        """
        更新电视剧订阅缺失集数
        """
        if not seasoninfo:
            return
        self.dbhelper.update_rss_tv_lack(rssid=rss_info.get('id'), lack_episodes=seasoninfo.get("episodes"), episode_filter_orders=seasoninfo.get('episode_filter_orders'))
        log.info("【Subscribe】更新电视剧 %s %s 缺失集数为 %s，各集版本优先级Index为 %s" % (
            rss_info.get('name'),
            rss_info.get('season'),
            len(seasoninfo.get("episodes")),
            "，".join([f'第{key}集: {value}' for key, value in seasoninfo.get("episode_filter_orders").items()]),
        ))

    def get_no_exists(self, media_info, rss_info, over_edition=False):
        """
        获取媒体信息的存在情况
        """
        exist_flag = False
        if media_info.type == MediaType.MOVIE:
            exist_flag, _, _ = self.downloader.check_exists_medias(
                meta_info=media_info,
            )
        else:
            # 从登记薄中获取缺失剧集
            season = 1
            if rss_info.get("season"):
                season = int(str(rss_info.get("season")).replace("S", ""))
            # 自定义集数
            total_ep = rss_info.get("total")
            current_ep = rss_info.get("current_ep")
            # 表中记录的剩余订阅集数
            episodes, episode_filter_orders = self.get_subscribe_tv_episodes(rss_info.get("id"))
            if not episode_filter_orders:
                episode_filter_orders = {k: 0 for k in range(current_ep or 1, total_ep+1)}
            if not episodes:
                episodes = list(range(current_ep or 1, total_ep + 1))
            rss_no_exists = {
                "season": season,
                "episodes": episodes,
                "total_episodes": total_ep,
                "episode_filter_orders": episode_filter_orders,
            }
            if not over_edition:
                exist_flag, local_no_exists, _ = self.downloader.check_exists_medias(
                    meta_info=media_info,
                )
                if local_no_exists:
                    local_no_exists = local_no_exists[0]
                    rss_no_exists['episodes'] = set(rss_no_exists['episodes']).intersection(local_no_exists['episodes'])
            # 更新本地和媒体库标记
            log.info("【Subscribe】订阅电视剧 %s %s 当前缺失集数为 %s，当前各集版本优先级Index为 %s" % (
                rss_info.get('name'),
                rss_info.get('season'),
                len(rss_no_exists.get("episodes")),
                "，".join([f'第{key}集: {value}' for key, value in rss_no_exists.get("episode_filter_orders").items()]),
            ))
        return exist_flag, rss_no_exists

    def get_subscribe_tv_episodes(self, rssid):
        """
        查询数据库中订阅的电视剧缺失集数
        """
        return self.dbhelper.get_rss_tv_episodes(rssid)
