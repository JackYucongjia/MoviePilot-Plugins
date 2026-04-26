import base64
import datetime
import json
import time
from threading import Event
from typing import Any, Dict, List, Optional, Tuple

import pytz
import requests
import urllib3
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from app.chain.subscribe import SubscribeChain
from app.core.config import settings
from app.core.metainfo import MetaInfo
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import MediaType, NotificationType


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


DEFAULT_CUSTOM_LISTS = [
    {"name": "IMDb Top 250 Movies", "id": "8647021", "type": "Movie", "mp_subscribe": True},
    {"name": "IMDb Top 250 TV Shows", "id": "8647022", "type": "Series", "mp_subscribe": False},
    {"name": "豆瓣电影 Top 250", "id": "8647023", "type": "Movie", "mp_subscribe": True},
]

DEFAULT_DOUBAN_GENRE_LISTS = [
    {"name": "豆瓣电影 - 剧情 - Top 20", "id": "8647681", "mp_subscribe": False},
    {"name": "豆瓣电影 - 喜剧 - Top 20", "id": "8647682", "mp_subscribe": False},
    {"name": "豆瓣电影 - 动作 - Top 20", "id": "8647683", "mp_subscribe": False},
    {"name": "豆瓣电影 - 爱情 - Top 20", "id": "8647684", "mp_subscribe": False},
    {"name": "豆瓣电影 - 科幻 - Top 20", "id": "8647685", "mp_subscribe": False},
    {"name": "豆瓣电影 - 动画 - Top 20", "id": "8647686", "mp_subscribe": False},
    {"name": "豆瓣电影 - 悬疑 - Top 20", "id": "8647687", "mp_subscribe": False},
    {"name": "豆瓣电影 - 惊悚 - Top 20", "id": "8647688", "mp_subscribe": False},
    {"name": "豆瓣电影 - 恐怖 - Top 20", "id": "8647689", "mp_subscribe": False},
    {"name": "豆瓣电影 - 纪录片 - Top 20", "id": "8647690", "mp_subscribe": False},
    {"name": "豆瓣电影 - 短片 - Top 20", "id": "8647691", "mp_subscribe": False},
    {"name": "豆瓣电影 - 情色 - Top 20", "id": "8647692", "mp_subscribe": False},
    {"name": "豆瓣电影 - 音乐 - Top 20", "id": "8647693", "mp_subscribe": False},
    {"name": "豆瓣电影 - 歌舞 - Top 20", "id": "8647694", "mp_subscribe": False},
    {"name": "豆瓣电影 - 家庭 - Top 20", "id": "8647695", "mp_subscribe": False},
    {"name": "豆瓣电影 - 儿童 - Top 20", "id": "8647696", "mp_subscribe": False},
    {"name": "豆瓣电影 - 传记 - Top 20", "id": "8647697", "mp_subscribe": False},
    {"name": "豆瓣电影 - 历史 - Top 20", "id": "8647698", "mp_subscribe": False},
    {"name": "豆瓣电影 - 战争 - Top 20", "id": "8647699", "mp_subscribe": False},
    {"name": "豆瓣电影 - 犯罪 - Top 20", "id": "8647700", "mp_subscribe": False},
    {"name": "豆瓣电影 - 西部 - Top 20", "id": "8647702", "mp_subscribe": False},
    {"name": "豆瓣电影 - 奇幻 - Top 20", "id": "8647703", "mp_subscribe": False},
    {"name": "豆瓣电影 - 冒险 - Top 20", "id": "8647704", "mp_subscribe": False},
    {"name": "豆瓣电影 - 灾难 - Top 20", "id": "8647705", "mp_subscribe": False},
    {"name": "豆瓣电影 - 武侠 - Top 20", "id": "8647706", "mp_subscribe": False},
    {"name": "豆瓣电影 - 古装 - Top 20", "id": "8647707", "mp_subscribe": False},
    {"name": "豆瓣电影 - 运动 - Top 20", "id": "8647708", "mp_subscribe": False},
    {"name": "豆瓣电影 - 黑色电影 - Top 20", "id": "8647709", "mp_subscribe": False},
]


class EmbyCollectionSync(_PluginBase):
    plugin_name = "Emby榜单合集同步"
    plugin_desc = "同步TMDb榜单到Emby合集，并将缺失媒体加入MoviePilot订阅。"
    plugin_icon = "movie.jpg"
    plugin_version = "0.1.0"
    plugin_author = "ffinly/codex"
    author_url = "https://github.com/ffinly/emby-collection-sync"
    plugin_config_prefix = "embycollectionsync_"
    plugin_order = 66
    auth_level = 2

    _event = Event()
    _scheduler: Optional[BackgroundScheduler] = None

    _enabled = False
    _onlyonce = False
    _notify = True
    _cron = "30 8 * * *"
    _emby_url = ""
    _emby_api_key = ""
    _tmdb_api_key = ""
    _use_proxy = False
    _proxy_url = ""
    _verify_ssl = False
    _batch_size = 50
    _sync_genre_lists = True
    _sync_domestic = True
    _fix_missing_posters = True
    _favorite_all_users = True
    _subscribe_missing = False
    _custom_lists = DEFAULT_CUSTOM_LISTS
    _genre_lists = DEFAULT_DOUBAN_GENRE_LISTS
    _path_keywords = ["国产", "华语", "Chinese"]
    _domestic_keywords = ["China", "Hong Kong", "Taiwan", "Macao", "中国", "香港", "台湾", "澳门", "CN", "HK", "TW"]
    _tmdb_domestic_codes = ["CN", "HK", "TW"]

    def init_plugin(self, config: dict = None):
        self.stop_service()

        if config:
            self._enabled = bool(config.get("enabled"))
            self._onlyonce = bool(config.get("onlyonce"))
            self._notify = bool(config.get("notify", True))
            self._cron = config.get("cron") or "30 8 * * *"
            self._emby_url = (config.get("emby_url") or "").rstrip("/")
            self._emby_api_key = config.get("emby_api_key") or ""
            self._tmdb_api_key = config.get("tmdb_api_key") or ""
            self._use_proxy = bool(config.get("use_proxy"))
            self._proxy_url = config.get("proxy_url") or ""
            self._verify_ssl = bool(config.get("verify_ssl"))
            self._batch_size = self.__int(config.get("batch_size"), 50)
            self._sync_genre_lists = bool(config.get("sync_genre_lists", True))
            self._sync_domestic = bool(config.get("sync_domestic", True))
            self._fix_missing_posters = bool(config.get("fix_missing_posters", True))
            self._favorite_all_users = bool(config.get("favorite_all_users", True))
            self._subscribe_missing = bool(config.get("subscribe_missing"))
            self._custom_lists = self.__load_json_list(config.get("custom_lists"), DEFAULT_CUSTOM_LISTS)
            self._genre_lists = self.__load_json_list(config.get("genre_lists"), DEFAULT_DOUBAN_GENRE_LISTS)
            self._path_keywords = self.__load_lines(config.get("path_keywords"), self._path_keywords)
            self._domestic_keywords = self.__load_lines(config.get("domestic_keywords"), self._domestic_keywords)
            self._tmdb_domestic_codes = self.__load_lines(config.get("tmdb_domestic_codes"), self._tmdb_domestic_codes)

        if self._enabled and self._onlyonce:
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            logger.info("Emby榜单合集同步服务启动，立即运行一次")
            self._scheduler.add_job(
                func=self.__sync,
                trigger="date",
                run_date=datetime.datetime.now(tz=pytz.timezone(settings.TZ)) + datetime.timedelta(seconds=3),
                name="Emby榜单合集同步",
            )
            self._onlyonce = False
            self.__update_config()
            if self._scheduler.get_jobs():
                self._scheduler.print_jobs()
                self._scheduler.start()

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        return []

    def get_service(self) -> List[Dict[str, Any]]:
        if self._enabled and self._cron:
            return [{
                "id": "EmbyCollectionSync",
                "name": "Emby榜单合集同步服务",
                "trigger": CronTrigger.from_crontab(self._cron),
                "func": self.__sync,
                "kwargs": {},
            }]
        return []

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return [
            {
                "component": "VForm",
                "content": [
                    self.__row([
                        self.__switch("enabled", "启用插件"),
                        self.__switch("onlyonce", "立即运行一次"),
                        self.__switch("notify", "发送通知"),
                    ]),
                    self.__row([
                        self.__text("emby_url", "Emby地址", "http://192.168.1.100:8096"),
                        self.__text("emby_api_key", "Emby API Key", "Emby控制台生成的API密钥"),
                        self.__text("tmdb_api_key", "TMDb API Key", "TMDb v3 API Key"),
                    ]),
                    self.__row([
                        self.__cron("cron", "执行周期", "30 8 * * *"),
                        self.__text("batch_size", "批处理大小", "50"),
                        self.__text("proxy_url", "TMDb代理", "http://host:port"),
                    ]),
                    self.__row([
                        self.__switch("use_proxy", "启用代理"),
                        self.__switch("verify_ssl", "校验SSL证书"),
                        self.__switch("subscribe_missing", "缺失项加入MP订阅"),
                    ]),
                    self.__row([
                        self.__switch("sync_genre_lists", "同步豆瓣分类榜"),
                        self.__switch("sync_domestic", "生成国产影视合集"),
                        self.__switch("fix_missing_posters", "修复无封面合集"),
                    ]),
                    self.__row([self.__switch("favorite_all_users", "生成合集加入所有用户收藏")]),
                    self.__textarea("custom_lists", "核心榜单JSON", "榜单数组：name/id/type/mp_subscribe"),
                    self.__textarea("genre_lists", "分类榜单JSON", "豆瓣分类榜数组"),
                    self.__textarea("path_keywords", "国产电影路径关键词", "每行一个关键词"),
                    self.__textarea("domestic_keywords", "国产电影产地关键词", "每行一个关键词"),
                    self.__textarea("tmdb_domestic_codes", "国产剧TMDb产地代码", "每行一个国家/地区代码"),
                ],
            }
        ], {
            "enabled": False,
            "onlyonce": False,
            "notify": True,
            "cron": "30 8 * * *",
            "emby_url": "",
            "emby_api_key": "",
            "tmdb_api_key": "",
            "use_proxy": False,
            "proxy_url": "",
            "verify_ssl": False,
            "batch_size": 50,
            "sync_genre_lists": True,
            "sync_domestic": True,
            "fix_missing_posters": True,
            "favorite_all_users": True,
            "subscribe_missing": False,
            "custom_lists": json.dumps(DEFAULT_CUSTOM_LISTS, ensure_ascii=False, indent=2),
            "genre_lists": json.dumps(DEFAULT_DOUBAN_GENRE_LISTS, ensure_ascii=False, indent=2),
            "path_keywords": "\n".join(self._path_keywords),
            "domestic_keywords": "\n".join(self._domestic_keywords),
            "tmdb_domestic_codes": "\n".join(self._tmdb_domestic_codes),
        }

    def get_page(self) -> List[dict]:
        history = self.get_data("history") or []
        if not history:
            return [{"component": "div", "text": "暂无同步记录", "props": {"class": "text-center"}}]

        items = []
        for record in sorted(history, key=lambda x: x.get("time", ""), reverse=True)[:20]:
            lines = [
                f"核心榜单：{record.get('core_lists', 0)}",
                f"分类榜单：{record.get('genre_lists', 0)}",
                f"国产电影/剧集：{record.get('domestic_movies', 0)}/{record.get('domestic_series', 0)}",
                f"MP订阅：新增 {record.get('mp_subscribed', 0)}，已存在 {record.get('mp_existed', 0)}，失败 {record.get('mp_failed', 0)}",
                f"封面修复：{record.get('fixed_covers', 0)}",
            ]
            items.append({
                "component": "VCard",
                "props": {"variant": "tonal"},
                "content": [
                    {"component": "VCardTitle", "text": record.get("time", "")},
                    {"component": "VCardText", "text": "\n".join(lines), "props": {"class": "whitespace-pre-line"}},
                ],
            })
        return [{"component": "div", "props": {"class": "grid gap-3 grid-info-card"}, "content": items}]

    def stop_service(self):
        try:
            if self._scheduler:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._event.set()
                    self._scheduler.shutdown()
                    self._event.clear()
                self._scheduler = None
        except Exception as err:
            logger.error(f"停止Emby榜单合集同步服务失败：{err}")

    def __sync(self):
        if self._event.is_set():
            return
        if not self._emby_url or not self._emby_api_key or not self._tmdb_api_key:
            logger.error("Emby榜单合集同步缺少 Emby 地址/API Key 或 TMDb API Key")
            return

        logger.info("开始执行Emby榜单合集同步")
        started_at = time.time()
        stats = self.__new_stats()
        client = self.__session()

        try:
            if self._sync_genre_lists:
                for list_info in reversed(self._genre_lists):
                    self.__process_list(client, list_info, stats, is_genre=True)

            if self._sync_domestic:
                self.__process_domestic(client, stats)

            for list_info in reversed(self._custom_lists):
                self.__process_list(client, list_info, stats, is_genre=False)

            if self._fix_missing_posters:
                self.__fix_missing_collection_posters(client, stats)

            stats["elapsed"] = int(time.time() - started_at)
            self.__save_history(stats)
            if self._notify:
                self.__send_report(stats)
            logger.info("Emby榜单合集同步执行完成")
        except Exception as err:
            logger.exception(f"Emby榜单合集同步执行失败：{err}")
            if self._notify:
                self.post_message(
                    mtype=NotificationType.Plugin,
                    title="【Emby榜单合集同步失败】",
                    text=str(err),
                )

    def __process_list(self, client: requests.Session, list_info: Dict[str, Any], stats: Dict[str, Any], is_genre: bool):
        name = list_info.get("name")
        list_id = str(list_info.get("id") or "")
        item_type = list_info.get("type", "Movie")
        tmdb_type = "movie" if item_type == "Movie" else "tv"
        if not name or not list_id:
            return

        logger.info(f"同步TMDb榜单：{name}")
        tmdb_items, list_desc = self.__fetch_tmdb_list_data(client, list_id)
        if not tmdb_items:
            stats["lists_report"][name] = {"is_genre": is_genre, "total": 0, "matched": 0, "missing": []}
            return

        poster_path = self.__get_original_poster(client, tmdb_items[0].get("id"), tmdb_type)
        emby_items = self.__get_emby_items(client, item_type, "ProviderIds,Name")
        emby_tmdb_map = {
            str(item.get("ProviderIds", {}).get("Tmdb")): item.get("Id")
            for item in emby_items
            if item.get("ProviderIds", {}).get("Tmdb")
        }

        matched_ids = []
        missing_items = []
        mp_sub_switch = bool(list_info.get("mp_subscribe"))
        for index, item in enumerate(tmdb_items, 1):
            tmdb_id = str(item.get("id") or "")
            title = item.get("title") or item.get("name") or "未知名称"
            year = (item.get("release_date") or item.get("first_air_date") or "")[:4]
            if tmdb_id in emby_tmdb_map:
                matched_ids.append(emby_tmdb_map[tmdb_id])
            else:
                missing_info = f"No.{index} {title} ({year}) {{tmdb-{tmdb_id}}}"
                missing_items.append(missing_info)
                if self._subscribe_missing and mp_sub_switch:
                    self.__subscribe_to_moviepilot(title, year, tmdb_id, item_type, stats)

        unique_ids = list(dict.fromkeys(matched_ids))
        if len(unique_ids) >= 2:
            self.__update_collection_by_name(client, name, unique_ids, list_desc, poster_path, stats)

        stats["lists_report"][name] = {
            "is_genre": is_genre,
            "total": len(tmdb_items),
            "matched": len(matched_ids),
            "missing": missing_items,
        }

    def __process_domestic(self, client: requests.Session, stats: Dict[str, Any]):
        all_series = self.__get_emby_items(client, "Series")
        domestic_series = []
        for series in all_series:
            tmdb_id = series.get("ProviderIds", {}).get("Tmdb")
            if not tmdb_id:
                continue
            try:
                data = self.__tmdb_get(client, f"https://api.themoviedb.org/3/tv/{tmdb_id}", timeout=5)
                if any(code in self._tmdb_domestic_codes for code in data.get("origin_country", [])):
                    domestic_series.append(series)
            except Exception:
                continue

        domestic_series.sort(key=lambda x: x.get("PremiereDate", "0000-00-00"), reverse=True)
        series_poster = ""
        if domestic_series:
            first_tmdb = domestic_series[0].get("ProviderIds", {}).get("Tmdb")
            series_poster = self.__get_original_poster(client, first_tmdb, "tv")
        self.__update_collection_by_name(client, "国产电视剧", [s["Id"] for s in domestic_series], "", series_poster, stats)
        stats["domestic_series"] = len(domestic_series)

        all_movies = self.__get_emby_items(client, "Movie")
        domestic_movies = [
            movie for movie in all_movies
            if any(keyword in movie.get("Path", "") for keyword in self._path_keywords)
            or any(
                any(keyword.lower() in location.lower() for keyword in self._domestic_keywords)
                for location in movie.get("ProductionLocations", [])
            )
        ]
        domestic_movies.sort(key=lambda x: x.get("PremiereDate", "0000-00-00"), reverse=True)
        movie_poster = ""
        if domestic_movies:
            first_tmdb = domestic_movies[0].get("ProviderIds", {}).get("Tmdb")
            movie_poster = self.__get_original_poster(client, first_tmdb, "movie")
        self.__update_collection_by_name(client, "国产电影", [m["Id"] for m in domestic_movies], "", movie_poster, stats)
        stats["domestic_movies"] = len(domestic_movies)

    def __fetch_tmdb_list_data(self, client: requests.Session, list_id: str) -> Tuple[List[dict], str]:
        items = []
        description = ""
        page = 1
        while True:
            data = None
            url = f"https://api.themoviedb.org/3/list/{list_id}"
            try:
                res = client.get(url, params={"api_key": self._tmdb_api_key, "language": "zh-CN", "page": page},
                                 proxies=self.__proxies(), timeout=15)
                if res.status_code == 404:
                    res = client.get(f"https://api.themoviedb.org/4/list/{list_id}",
                                     params={"api_key": self._tmdb_api_key, "language": "zh-CN", "page": page},
                                     proxies=self.__proxies(), timeout=15)
                if res.status_code != 200:
                    logger.error(f"获取TMDb榜单失败：{list_id} HTTP {res.status_code} {res.text[:200]}")
                    break
                data = res.json()
            except Exception as err:
                logger.error(f"获取TMDb榜单异常：{list_id} {err}")
                break

            if page == 1:
                description = data.get("description", "")
            page_items = data.get("results") or data.get("items") or []
            if not page_items:
                break
            items.extend(page_items)
            if page >= data.get("total_pages", 1):
                break
            page += 1
            time.sleep(0.2)
        return items, description

    def __get_original_poster(self, client: requests.Session, tmdb_id: Any, item_type: str = "movie") -> str:
        if not tmdb_id:
            return ""
        try:
            detail = self.__tmdb_get(client, f"https://api.themoviedb.org/3/{item_type}/{tmdb_id}", timeout=8)
            original_language = detail.get("original_language", "en")
            images = self.__tmdb_get(client, f"https://api.themoviedb.org/3/{item_type}/{tmdb_id}/images", timeout=8)
            posters = images.get("posters", [])
            for poster in posters:
                if poster.get("iso_639_1") == original_language:
                    return poster.get("file_path", "")
            if posters:
                return posters[0].get("file_path", "")
            return detail.get("poster_path", "")
        except Exception:
            return ""

    def __update_collection_by_name(
        self,
        client: requests.Session,
        name: str,
        item_ids: List[str],
        list_desc: str,
        poster_path: str,
        stats: Dict[str, Any],
    ):
        if len(item_ids) < 2:
            return
        try:
            search_res = client.get(f"{self._emby_url}/emby/Items", params={
                "api_key": self._emby_api_key,
                "IncludeItemTypes": "BoxSet",
                "SearchTerm": name,
                "Recursive": True,
            }, timeout=15).json()
            existing = next((item for item in search_res.get("Items", []) if item.get("Name") == name), None)
            if existing:
                client.delete(f"{self._emby_url}/emby/Items/{existing['Id']}", params={"api_key": self._emby_api_key}, timeout=15)

            create_res = client.post(f"{self._emby_url}/emby/Collections", params={
                "api_key": self._emby_api_key,
                "Name": name,
                "Ids": ",".join(item_ids[:self._batch_size]),
            }, timeout=30).json()
            collection_id = create_res.get("Id")
            if not collection_id:
                logger.error(f"创建Emby合集失败：{name} {create_res}")
                return

            for index in range(self._batch_size, len(item_ids), self._batch_size):
                client.post(f"{self._emby_url}/emby/Collections/{collection_id}/Items", params={
                    "api_key": self._emby_api_key,
                    "Ids": ",".join(item_ids[index:index + self._batch_size]),
                }, timeout=30)
                time.sleep(0.1)

            if list_desc:
                self.__update_collection_overview(client, collection_id, list_desc)
            if self._favorite_all_users:
                self.__add_to_all_users_favorites(client, collection_id, name, stats)
            if poster_path and not self.__upload_poster_to_emby(client, collection_id, poster_path):
                stats["poster_failed"].append(name)
            logger.info(f"已生成Emby合集：{name}，共 {len(item_ids)} 个项目")
        except Exception as err:
            logger.error(f"更新Emby合集失败：{name} {err}")

    def __fix_missing_collection_posters(self, client: requests.Session, stats: Dict[str, Any]):
        exclude_names = [item["name"] for item in self._custom_lists] + [item["name"] for item in self._genre_lists] + ["国产电影", "国产电视剧"]
        collections = client.get(f"{self._emby_url}/emby/Items", params={
            "api_key": self._emby_api_key,
            "IncludeItemTypes": "BoxSet",
            "Recursive": True,
            "Fields": "ImageTags",
        }, timeout=30).json().get("Items", [])

        for collection in collections:
            if collection.get("Name") in exclude_names or "Primary" in collection.get("ImageTags", {}):
                continue
            items = client.get(f"{self._emby_url}/emby/Items", params={
                "api_key": self._emby_api_key,
                "ParentId": collection["Id"],
                "Fields": "PremiereDate,ProviderIds",
                "SortBy": "PremiereDate",
                "SortOrder": "Ascending",
            }, timeout=30).json().get("Items", [])
            for item in items:
                tmdb_id = item.get("ProviderIds", {}).get("Tmdb")
                if not tmdb_id:
                    continue
                item_type = "movie" if item.get("Type") == "Movie" else "tv"
                poster_path = self.__get_original_poster(client, tmdb_id, item_type)
                if self.__upload_poster_to_emby(client, collection["Id"], poster_path):
                    stats["fixed_covers"] += 1
                else:
                    stats["poster_failed"].append(collection.get("Name"))
                break

    def __subscribe_to_moviepilot(self, title: str, year: str, tmdb_id: str, item_type: str, stats: Dict[str, Any]):
        try:
            mtype = MediaType.MOVIE if item_type == "Movie" else MediaType.TV
            meta = MetaInfo(title)
            meta.year = year
            meta.type = mtype
            mediainfo = self.chain.recognize_media(meta=meta, tmdbid=int(tmdb_id))
            subscribe_chain = SubscribeChain()
            if mediainfo and subscribe_chain.exists(mediainfo=mediainfo, meta=meta):
                stats["mp_existed"] += 1
                return
            subscribe_chain.add(
                title=mediainfo.title if mediainfo else title,
                year=mediainfo.year if mediainfo else year,
                mtype=mediainfo.type if mediainfo else mtype,
                tmdbid=mediainfo.tmdb_id if mediainfo else int(tmdb_id),
                exist_ok=True,
                username=self.plugin_name,
            )
            stats["mp_subscribed"] += 1
        except Exception as err:
            stats["mp_failed"].append(f"{title} ({tmdb_id}): {err}")

    def __upload_poster_to_emby(self, client: requests.Session, collection_id: str, poster_path: str) -> bool:
        if not poster_path:
            return False
        try:
            image_res = client.get(f"https://image.tmdb.org/t/p/original{poster_path}",
                                   proxies=self.__proxies(), timeout=20)
            if image_res.status_code != 200:
                return False
            b64_image = base64.b64encode(image_res.content).decode("utf-8")
            mime_type = image_res.headers.get("Content-Type", "image/jpeg")
            try:
                client.delete(f"{self._emby_url}/emby/Items/{collection_id}/Images/Primary",
                              params={"api_key": self._emby_api_key}, timeout=8)
            except Exception:
                pass
            res = client.post(
                f"{self._emby_url}/emby/Items/{collection_id}/Images/Primary",
                params={"api_key": self._emby_api_key},
                data=b64_image,
                headers={"Content-Type": mime_type},
                proxies={"http": None, "https": None},
                timeout=15,
            )
            if res.status_code not in (200, 204):
                return False
            time.sleep(1)
            check = client.get(f"{self._emby_url}/emby/Items", params={
                "api_key": self._emby_api_key,
                "Ids": collection_id,
                "Fields": "ImageTags",
            }, timeout=8).json()
            items = check.get("Items", [])
            return bool(items and "Primary" in items[0].get("ImageTags", {}))
        except Exception:
            return False

    def __update_collection_overview(self, client: requests.Session, collection_id: str, overview: str):
        users = self.__get_emby_users(client)
        if not users:
            return
        try:
            item_info = client.get(f"{self._emby_url}/emby/Users/{users[0]['Id']}/Items/{collection_id}",
                                   params={"api_key": self._emby_api_key}, timeout=15).json()
            item_info["Overview"] = overview
            client.post(f"{self._emby_url}/emby/Items/{collection_id}",
                        params={"api_key": self._emby_api_key}, json=item_info, timeout=15)
        except Exception as err:
            logger.warning(f"写入Emby合集简介失败：{err}")

    def __add_to_all_users_favorites(self, client: requests.Session, item_id: str, item_name: str, stats: Dict[str, Any]):
        for user in self.__get_emby_users(client):
            try:
                user_item = client.get(f"{self._emby_url}/emby/Users/{user['Id']}/Items/{item_id}",
                                       params={"api_key": self._emby_api_key}, timeout=10).json()
                if user_item.get("UserData", {}).get("IsFavorite"):
                    continue
                client.post(f"{self._emby_url}/emby/Users/{user['Id']}/FavoriteItems/{item_id}",
                            params={"api_key": self._emby_api_key}, timeout=10)
                stats["favorites"] += 1
            except Exception:
                logger.debug(f"添加合集到用户收藏失败：{item_name}")

    def __get_emby_users(self, client: requests.Session) -> List[dict]:
        try:
            return client.get(f"{self._emby_url}/emby/Users", params={"api_key": self._emby_api_key}, timeout=15).json()
        except Exception:
            return []

    def __get_emby_items(self, client: requests.Session, item_type: str, fields: str = "ProductionLocations,Path,ProviderIds,PremiereDate") -> List[dict]:
        try:
            return client.get(f"{self._emby_url}/emby/Items", params={
                "api_key": self._emby_api_key,
                "IncludeItemTypes": item_type,
                "Recursive": True,
                "Fields": fields,
                "Limit": 20000,
            }, timeout=60).json().get("Items", [])
        except Exception as err:
            logger.error(f"获取Emby项目失败：{item_type} {err}")
            return []

    def __tmdb_get(self, client: requests.Session, url: str, timeout: int = 10) -> dict:
        return client.get(url, params={"api_key": self._tmdb_api_key}, proxies=self.__proxies(), timeout=timeout).json()

    def __session(self) -> requests.Session:
        client = requests.Session()
        retry = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        client.mount("http://", HTTPAdapter(max_retries=retry))
        client.mount("https://", HTTPAdapter(max_retries=retry))
        client.verify = self._verify_ssl
        return client

    def __proxies(self) -> Optional[dict]:
        if self._use_proxy and self._proxy_url:
            return {"http": self._proxy_url, "https": self._proxy_url}
        return None

    def __send_report(self, stats: Dict[str, Any]):
        core_lines = []
        genre_matched = 0
        genre_missing = []
        for list_name, data in stats["lists_report"].items():
            if data.get("is_genre"):
                if data["matched"] >= 2:
                    genre_matched += 1
                if data["missing"]:
                    short_name = list_name.replace("豆瓣电影 - ", "").replace(" - Top 20", "")
                    genre_missing.append(f"{short_name}(缺{len(data['missing'])})")
            else:
                core_lines.append(f"{list_name}: 总{data['total']} / 已有{data['matched']} / 缺{len(data['missing'])}")

        lines = [
            "Emby榜单合集同步完成",
            *core_lines,
            f"豆瓣分类合集：生成 {genre_matched} 个",
            f"国产电影/剧集：{stats['domestic_movies']} / {stats['domestic_series']}",
            f"MP订阅：新增 {stats['mp_subscribed']}，已存在 {stats['mp_existed']}，失败 {len(stats['mp_failed'])}",
            f"全员收藏人次：{stats['favorites']}",
            f"无封面合集修复：{stats['fixed_covers']}",
            f"耗时：{stats['elapsed']} 秒",
        ]
        for list_name, data in stats["lists_report"].items():
            if not data.get("is_genre") and data["missing"]:
                lines.append(f"\n【{list_name} 缺失清单】")
                lines.extend([f"- {item}" for item in data["missing"][:15]])
                if len(data["missing"]) > 15:
                    lines.append(f"- ... 等共 {len(data['missing'])} 部")
        if genre_missing:
            lines.append("\n【豆瓣分类缺失概览】\n" + " | ".join(genre_missing))
        if stats["mp_failed"]:
            lines.append("\n【MP订阅失败】")
            lines.extend([f"- {item}" for item in stats["mp_failed"][:15]])
        if stats["poster_failed"]:
            lines.append("\n【海报注入失败】")
            lines.extend([f"- {item}" for item in stats["poster_failed"][:15]])

        self.post_message(
            mtype=NotificationType.Plugin,
            title="【Emby榜单合集同步报告】",
            text="\n".join(lines),
        )

    def __save_history(self, stats: Dict[str, Any]):
        history = self.get_data("history") or []
        history.append({
            "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "core_lists": len([v for v in stats["lists_report"].values() if not v.get("is_genre")]),
            "genre_lists": len([v for v in stats["lists_report"].values() if v.get("is_genre") and v.get("matched", 0) >= 2]),
            "domestic_movies": stats["domestic_movies"],
            "domestic_series": stats["domestic_series"],
            "mp_subscribed": stats["mp_subscribed"],
            "mp_existed": stats["mp_existed"],
            "mp_failed": len(stats["mp_failed"]),
            "fixed_covers": stats["fixed_covers"],
        })
        self.save_data("history", history[-20:])

    def __update_config(self):
        self.update_config({
            "enabled": self._enabled,
            "onlyonce": self._onlyonce,
            "notify": self._notify,
            "cron": self._cron,
            "emby_url": self._emby_url,
            "emby_api_key": self._emby_api_key,
            "tmdb_api_key": self._tmdb_api_key,
            "use_proxy": self._use_proxy,
            "proxy_url": self._proxy_url,
            "verify_ssl": self._verify_ssl,
            "batch_size": self._batch_size,
            "sync_genre_lists": self._sync_genre_lists,
            "sync_domestic": self._sync_domestic,
            "fix_missing_posters": self._fix_missing_posters,
            "favorite_all_users": self._favorite_all_users,
            "subscribe_missing": self._subscribe_missing,
            "custom_lists": json.dumps(self._custom_lists, ensure_ascii=False, indent=2),
            "genre_lists": json.dumps(self._genre_lists, ensure_ascii=False, indent=2),
            "path_keywords": "\n".join(self._path_keywords),
            "domestic_keywords": "\n".join(self._domestic_keywords),
            "tmdb_domestic_codes": "\n".join(self._tmdb_domestic_codes),
        })

    @staticmethod
    def __new_stats() -> Dict[str, Any]:
        return {
            "domestic_movies": 0,
            "domestic_series": 0,
            "favorites": 0,
            "fixed_covers": 0,
            "mp_subscribed": 0,
            "mp_existed": 0,
            "mp_failed": [],
            "poster_failed": [],
            "lists_report": {},
            "elapsed": 0,
        }

    @staticmethod
    def __load_json_list(value: Any, default: List[dict]) -> List[dict]:
        if isinstance(value, list):
            return value
        if not value:
            return default
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else default
        except Exception:
            return default

    @staticmethod
    def __load_lines(value: Any, default: List[str]) -> List[str]:
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
        if not value:
            return default
        return [line.strip() for line in str(value).splitlines() if line.strip()]

    @staticmethod
    def __int(value: Any, default: int) -> int:
        try:
            return int(value)
        except Exception:
            return default

    @staticmethod
    def __row(cols: List[dict]) -> dict:
        return {"component": "VRow", "content": cols}

    @staticmethod
    def __switch(model: str, label: str) -> dict:
        return {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [{"component": "VSwitch", "props": {"model": model, "label": label}}]}

    @staticmethod
    def __text(model: str, label: str, placeholder: str = "") -> dict:
        return {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [{"component": "VTextField", "props": {"model": model, "label": label, "placeholder": placeholder}}]}

    @staticmethod
    def __cron(model: str, label: str, placeholder: str = "") -> dict:
        return {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [{"component": "VCronField", "props": {"model": model, "label": label, "placeholder": placeholder}}]}

    @staticmethod
    def __textarea(model: str, label: str, placeholder: str = "") -> dict:
        return {
            "component": "VRow",
            "content": [{
                "component": "VCol",
                "props": {"cols": 12},
                "content": [{"component": "VTextarea", "props": {"model": model, "label": label, "placeholder": placeholder, "rows": 4}}],
            }],
        }
