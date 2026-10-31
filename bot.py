"""Bot đồng bộ 50 tin ban đầu, tự dọn bộ nhớ, đánh dấu đã sao chép và chỉ cập nhật tin nhắn mới."""
import asyncio
import io
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import discord
from discord.ext import tasks
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent
LOG = logging.getLogger("mirror")
LIMIT = 50


def read_config():
    cfg = json.loads((ROOT / "config.json").read_text(encoding="utf-8"))
    if not cfg.get("auto_discover_channels") and not cfg.get("source_channel_ids"):
        raise ValueError("Bật auto_discover_channels hoặc chọn source_channel_ids.")
    if cfg["source_guild_id"] == cfg["target_guild_id"]:
        raise ValueError("Hai server phải khác nhau.")
    return cfg


def message_payload(msg):
    return {
        "username": msg.author.name,
        "display_name": msg.author.display_name,
        "author_id": msg.author.id,
        "avatar": str(msg.author.display_avatar.url),
        "content": msg.content,
        "created_at": msg.created_at.isoformat(),
        "attachments": [
            {
                "name": a.filename,
                "url": a.url,
                "size": a.size,
                "content_type": a.content_type or "",
                "spoiler": a.is_spoiler(),
            }
            for a in msg.attachments
        ],
        "stickers": [str(s.url) for s in msg.stickers],
        "reply_to": msg.reference.message_id if msg.reference else None,
    }


def render_message(payload, source_id, fallback_links=None, deleted=False):
    has_media = bool(payload["attachments"] or payload["stickers"])
    body = payload["content"] or ("*(Đính kèm tệp / phương tiện)*" if has_media else "*(Không có nội dung chữ)*")
    if deleted:
        body = f"**[ĐÃ XÓA Ở SERVER NGUỒN]**\n{body}"
    colour = 0xED4245 if deleted else 0x5865F2

    embed = discord.Embed(description=body[:4096], colour=colour)
    author_prefix = "[ĐÃ XÓA] " if deleted else ""
    embed.set_author(
        name=f'{author_prefix}@{payload["username"]} · {payload["display_name"]}'[:256],
        icon_url=payload["avatar"],
    )
    status_text = "Đã xóa ở nguồn" if deleted else "Bản sao"
    embed.set_footer(text=f"{status_text} • ID nguồn: {source_id} • {payload['created_at']}")

    extra = []
    if payload["reply_to"]:
        extra.append(f'Trả lời tin ID {payload["reply_to"]}')
    if fallback_links:
        extra += [f'{a["name"]} (link gốc): {a["url"]}' for a in fallback_links]
    extra += payload["stickers"]

    if extra:
        embed.add_field(name="Thông tin đính kèm / trả lời", value="\n".join(extra)[:1024], inline=False)

    if payload["stickers"]:
        embed.set_thumbnail(url=payload["stickers"][0])

    return embed


async def prepare_attachments(msg, max_bytes):
    files = []
    fallback_links = []
    for a in msg.attachments:
        if a.size > max_bytes:
            fallback_links.append({"name": a.filename, "url": a.url})
            continue
        try:
            f = await a.to_file(spoiler=a.is_spoiler())
            files.append(f)
        except (discord.HTTPException, discord.NotFound, OSError) as exc:
            LOG.warning("Không tải được tệp %s: %s", a.filename, exc)
            fallback_links.append({"name": a.filename, "url": a.url})
    return files, fallback_links


def role_pages(guild):
    groups = {}
    for member in guild.members:
        groups.setdefault(member.top_role.id, []).append(member)
    pages = []
    for role in sorted(guild.roles, reverse=True):
        members = groups.get(role.id, [])
        if not members:
            continue
        heading = f"Role cao nhất: {discord.utils.escape_markdown(role.name)} ({len(members)})\n"
        page = heading
        for member in sorted(members, key=lambda x: (x.name.casefold(), x.id)):
            line = discord.utils.escape_markdown(f"@{member.name} · {member.display_name}") + "\n"
            if len(page) + len(line) > 1900:
                pages.append(page)
                page = heading + "(tiếp)\n"
            page += line
        pages.append(page)
    return pages or ["Chưa có thành viên trong danh sách."]


class Mirror(discord.Client):
    def __init__(self, cfg):
        intents = discord.Intents.none()
        intents.guilds = True
        intents.guild_messages = True
        intents.message_content = True
        intents.voice_states = True
        intents.members = True
        super().__init__(intents=intents, allowed_mentions=discord.AllowedMentions.none())
        self.cfg = cfg
        self.selected = set(cfg.get("source_channel_ids", []))
        path = ROOT / "state.json"
        try:
            content = path.read_text("utf-8").strip() if path.exists() else ""
            self.state = json.loads(content) if content else {}
        except json.JSONDecodeError:
            self.state = {}

        scope = [cfg["source_guild_id"], cfg["target_guild_id"]]
        if self.state and self.state.get("scope") and self.state.get("scope") != scope:
            LOG.warning("Phát hiện state.json của cặp server cũ, tự động làm mới.")
            self.state = {}

        self.state["scope"] = scope
        self.state.setdefault("channels", {})
        self.state.setdefault("categories", {})
        self.dirty = set(self.selected)
        self.enabled = False
        self.tick = 0
        self.role_lock = asyncio.Lock()

    def save(self):
        temp = ROOT / "state.tmp"
        temp.write_text(json.dumps(self.state, ensure_ascii=False, indent=2), encoding="utf-8")
        temp.replace(ROOT / "state.json")

    async def setup_hook(self):
        self.worker.start()
        self.role_worker.start()

    async def on_ready(self):
        self.enabled = False
        source = self.get_guild(self.cfg["source_guild_id"])
        target = self.get_guild(self.cfg["target_guild_id"])
        if not source or not target:
            LOG.error("Bot phải có mặt ở cả hai server (Nguồn: %s, Đích: %s).",
                      self.cfg["source_guild_id"], self.cfg["target_guild_id"])
            return
        try:
            self.discover(source)
            for cid in list(self.selected):
                ch = source.get_channel(cid)
                if not isinstance(ch, (discord.TextChannel, discord.VoiceChannel)):
                    LOG.warning("Bỏ qua kênh %s (không phải text/voice).", cid)
                    self.selected.discard(cid)
                    continue
                perms = ch.permissions_for(source.me)
                if not perms.view_channel or not perms.read_message_history:
                    LOG.warning("Thiếu quyền đọc kênh %s (%s), bỏ qua.", ch.name, cid)
                    self.selected.discard(cid)

            self.overwrites = {target.default_role: discord.PermissionOverwrite(view_channel=False)}
            for rid in self.cfg.get("target_viewer_role_ids", []):
                role = target.get_role(rid)
                if not role or role == target.default_role:
                    LOG.warning("Role đích không hợp lệ: %s", rid)
                    continue
                self.overwrites[role] = discord.PermissionOverwrite(
                    view_channel=True, read_message_history=True, send_messages=False, connect=False)
            self.overwrites[target.me] = discord.PermissionOverwrite(
                view_channel=True, read_message_history=True, send_messages=True,
                embed_links=True, attach_files=True, manage_channels=True, connect=True)
        except (ValueError, discord.HTTPException) as exc:
            LOG.error("Lỗi khởi tạo đồng bộ: %s", exc)
            return

        self.enabled = True
        self.dirty.update(self.selected)
        LOG.info("Sẵn sàng đồng bộ %d tin nhắn gần nhất cho %d kênh.", LIMIT, len(self.selected))

    def discover(self, source):
        if not self.cfg.get("auto_discover_channels"):
            return
        found = set()
        for ch in source.channels:
            if isinstance(ch, (discord.TextChannel, discord.VoiceChannel)):
                perms = ch.permissions_for(source.me)
                if perms.view_channel and perms.read_message_history:
                    found.add(ch.id)
        self.dirty.update(found - self.selected)
        self.selected = found

    async def get_role_log_channel(self, target):
        roster = self.state.setdefault("roster", {"pages": []})
        log_ch = target.get_channel(roster.get("log_channel_id", 0))
        if log_ch is None:
            log_ch = await target.create_text_channel(
                "update-role", overwrites=self.overwrites,
                topic="Lịch sử thêm và mất role của thành viên bên server nguồn.")
            roster["log_channel_id"] = log_ch.id
            self.save()
        return log_ch

    async def sync_role_roster(self, source):
        async with self.role_lock:
            target = self.get_guild(self.cfg["target_guild_id"])
            if not target:
                return
            roster = self.state.setdefault("roster", {"pages": []})

            channel = target.get_channel(roster.get("channel_id", 0))
            if channel is None:
                channel = await target.create_text_channel(
                    "danh-sach-role", overwrites=self.overwrites,
                    topic="Thành viên nguồn, nhóm theo role cao nhất. Tự động cập nhật mỗi 5s.")
                roster.update(channel_id=channel.id, pages=[])
                self.save()

            log_channel = await self.get_role_log_channel(target)

            pages = role_pages(source)
            for index, text in enumerate(pages):
                old = roster["pages"][index] if index < len(roster["pages"]) else None
                if old and old["text"] == text:
                    continue
                result = None
                if old:
                    try:
                        result = await channel.get_partial_message(old["id"]).edit(content=text)
                    except discord.NotFound:
                        pass
                if result is None:
                    result = await channel.send(text, silent=True)
                entry = {"id": result.id, "text": text}
                if old:
                    roster["pages"][index] = entry
                else:
                    roster["pages"].append(entry)
                self.save()

            while len(roster["pages"]) > len(pages):
                old = roster["pages"][-1]
                try:
                    await channel.get_partial_message(old["id"]).delete()
                except discord.NotFound:
                    pass
                roster["pages"].pop()
                self.save()

            old_members = {m["id"]: m for m in roster.get("members", [])}
            snapshot = []
            now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

            for member in sorted(source.members, key=lambda x: x.id):
                roles_data = [
                    {"id": role.id, "name": role.name}
                    for role in member.roles
                    if role != source.default_role
                ]
                snapshot.append({
                    "id": member.id,
                    "username": member.name,
                    "display_name": member.display_name,
                    "top_role_id": member.top_role.id,
                    "roles": roles_data,
                })

                if old_members and member.id in old_members:
                    old_r = {
                        r["id"]: r["name"]
                        for r in old_members[member.id].get("roles", [])
                        if r["id"] != source.default_role.id
                    }
                    new_r = {r["id"]: r["name"] for r in roles_data}

                    added_ids = set(new_r.keys()) - set(old_r.keys())
                    removed_ids = set(old_r.keys()) - set(new_r.keys())

                    if added_ids or removed_ids:
                        embed = discord.Embed(
                            title="Cập nhật Role thành viên",
                            colour=0x57F287 if added_ids and not removed_ids else (0xED4245 if removed_ids and not added_ids else 0xFEE75C),
                        )
                        embed.set_author(
                            name=f"@{member.name} · {member.display_name} (ID: {member.id})",
                            icon_url=str(member.display_avatar.url),
                        )
                        if added_ids:
                            added_names = ", ".join(f"**{new_r[rid]}**" for rid in added_ids)
                            embed.add_field(name="➕ Được thêm Role", value=added_names[:1024], inline=False)
                        if removed_ids:
                            removed_names = ", ".join(f"**{old_r[rid]}**" for rid in removed_ids)
                            embed.add_field(name="➖ Bị mất Role", value=removed_names[:1024], inline=False)
                        embed.set_footer(text=f"Thời gian: {now_str}")
                        await log_channel.send(embed=embed, silent=True)

            if roster.get("members") != snapshot:
                roster["members"] = snapshot
                self.save()

    async def destination(self, src):
        guild = self.get_guild(self.cfg["target_guild_id"])
        key = str(src.id)

        self.state.setdefault("channels", {})
        self.state.setdefault("categories", {})

        record = self.state["channels"].setdefault(
            key, {"id": 0, "initial_done": False, "last_synced_id": 0, "messages": {}}
        )
        record.setdefault("messages", {})
        record.setdefault("initial_done", False)
        record.setdefault("last_synced_id", 0)

        dst = guild.get_channel(record.get("id", 0))
        if dst:
            return dst, record
        category = None
        if src.category:
            ckey = str(src.category.id)
            category = guild.get_channel(self.state["categories"].get(ckey, 0))
            if category is None:
                category = await guild.create_category(src.category.name, overwrites=self.overwrites)
                self.state["categories"][ckey] = category.id
                self.save()
        create = guild.create_voice_channel if isinstance(src, discord.VoiceChannel) else guild.create_text_channel
        dst = await create(src.name, category=category, overwrites=self.overwrites,
                           reason="Đồng bộ 50 tin nhắn gần nhất từ server nguồn")
        record.update(id=dst.id, initial_done=False, last_synced_id=0, messages={})
        record.pop("voice_id", None)
        record.pop("voice_text", None)
        self.save()
        return dst, record

    async def mark_message_deleted(self, src_cid, mid):
        record = self.state.get("channels", {}).get(str(src_cid))
        if not record:
            return
        mapping = record.get("messages", {})
        item = mapping.get(str(mid))
        if not item or item.get("deleted"):
            return

        target = self.get_guild(self.cfg["target_guild_id"])
        if not target:
            return
        dst = target.get_channel(record.get("id", 0))
        if not dst:
            return

        item["deleted"] = True
        embed = render_message(
            item["payload"],
            str(mid),
            fallback_links=item.get("fallback_links"),
            deleted=True,
        )
        try:
            await dst.get_partial_message(item["id"]).edit(embed=embed)
        except discord.NotFound:
            pass
        self.save()

    async def sync_messages(self, src, dst, record):
        max_bytes = getattr(dst.guild, "filesize_limit", 25 * 1024 * 1024)
        mapping = record.setdefault("messages", {})

        # TRƯỜNG HỢP 1: Kênh chưa sao lưu lần đầu -> Sao chép 50 tin gần nhất rồi xóa data, đánh dấu hoàn tất
        if not record.get("initial_done"):
            recent = [m async for m in src.history(limit=LIMIT)]
            latest_id = max((m.id for m in recent), default=0)

            for msg in reversed(recent):
                mid = str(msg.id)
                payload = message_payload(msg)
                files, fallback_links = await prepare_attachments(msg, max_bytes)
                embed = render_message(payload, mid, fallback_links=fallback_links, deleted=False)
                try:
                    await dst.send(embed=embed, files=files, silent=True)
                except discord.HTTPException as exc:
                    LOG.warning("Upload file lỗi ở tin %s (%s), chuyển sang gửi link dự phòng.", mid, exc)
                    fallback_links = [{"name": a.filename, "url": a.url} for a in msg.attachments]
                    embed = render_message(payload, mid, fallback_links=fallback_links, deleted=False)
                    await dst.send(embed=embed, silent=True)
                await asyncio.sleep(0.35)

            # Xóa sạch dữ liệu tin nhắn vừa chép, chỉ lưu trạng thái đã sao chép và ID mốc cuối cùng
            record["messages"] = {}
            record["initial_done"] = True
            record["last_synced_id"] = latest_id
            self.save()
            LOG.info("Đã sao lưu xong 50 tin ban đầu cho kênh %s, xóa bộ nhớ đệm và chỉ chờ tin mới.", src.name)
            return

        # TRƯỜNG HỢP 2: Kênh đã sao lưu xong -> Chỉ cập nhật các tin nhắn mới phát sinh sau last_synced_id
        last_id = record.get("last_synced_id", 0)
        recent = [m async for m in src.history(limit=LIMIT)]
        keep = {str(m.id) for m in recent}
        oldest_recent_id = min((m.id for m in recent), default=0)

        # Kiểm tra các tin nhắn mới đang được theo dõi nếu bị xóa ở nguồn
        for mid in list(mapping):
            if mid not in keep:
                item = mapping[mid]
                if int(mid) > oldest_recent_id:
                    if not item.get("deleted"):
                        item["deleted"] = True
                        embed = render_message(
                            item["payload"],
                            mid,
                            fallback_links=item.get("fallback_links"),
                            deleted=True,
                        )
                        try:
                            await dst.get_partial_message(item["id"]).edit(embed=embed)
                            await asyncio.sleep(0.4)
                        except discord.NotFound:
                            pass
                        # Sau khi đánh dấu đã xóa xong thì dọn khỏi bộ nhớ luôn
                        mapping.pop(mid, None)
                        self.save()
                else:
                    mapping.pop(mid, None)
                    self.save()

        new_latest_id = last_id
        for msg in reversed(recent):
            mid = str(msg.id)
            # Bỏ qua hoàn toàn các tin nhắn cũ đã được sao chép trước đó
            if msg.id <= last_id and mid not in mapping:
                continue

            payload = message_payload(msg)
            existing = mapping.get(mid)
            if existing and existing["payload"] == payload and not existing.get("deleted"):
                continue

            result = None
            fallback_links = existing.get("fallback_links", []) if existing else []
            if existing:
                embed = render_message(payload, mid, fallback_links=fallback_links, deleted=False)
                try:
                    result = await dst.get_partial_message(existing["id"]).edit(embed=embed)
                except discord.NotFound:
                    pass

            if result is None:
                files, fallback_links = await prepare_attachments(msg, max_bytes)
                embed = render_message(payload, mid, fallback_links=fallback_links, deleted=False)
                try:
                    result = await dst.send(embed=embed, files=files, silent=True)
                except discord.HTTPException as exc:
                    LOG.warning("Upload file lỗi ở tin %s (%s), chuyển sang gửi link dự phòng.", mid, exc)
                    fallback_links = [{"name": a.filename, "url": a.url} for a in msg.attachments]
                    embed = render_message(payload, mid, fallback_links=fallback_links, deleted=False)
                    result = await dst.send(embed=embed, silent=True)

            if msg.id > new_latest_id:
                new_latest_id = msg.id

            # Lưu tạm tối đa 15 tin nhắn mới nhất để bắt sự kiện chỉnh sửa/xóa, tự động xóa bớt tin cũ hơn cho nhẹ file
            mapping[mid] = {
                "id": result.id,
                "payload": payload,
                "fallback_links": fallback_links,
                "deleted": False,
            }
            while len(mapping) > 15:
                oldest_key = min(mapping.keys(), key=lambda k: int(k))
                mapping.pop(oldest_key, None)

            record["last_synced_id"] = new_latest_id
            self.save()
            await asyncio.sleep(0.3)

    async def sync_voice(self, src, dst, record):
        lines = [f"@{m.name} · {m.display_name}" for m in sorted(src.members, key=lambda m: m.id)]
        full = f"Room thoại: {src.name}\nNhững người đang ngồi ({len(lines)}):\n" + (
            "\n".join(lines) or "Hiện không có ai trong phòng.")
        if record.get("voice_text") == full:
            return
        content = discord.utils.escape_markdown(full)
        files = []
        if len(content) > 2000:
            content = content[:1800] + "\n… Danh sách đầy đủ trong tệp đính kèm."
            files = [discord.File(io.BytesIO(full.encode("utf-8")), filename="nguoi-dang-ngoi.txt")]
        result = None
        if record.get("voice_id"):
            try:
                result = await dst.get_partial_message(record["voice_id"]).edit(
                    content=content, attachments=files)
            except discord.NotFound:
                pass
        if result is None:
            result = await dst.send(content, files=files, silent=True)
        record.update(voice_id=result.id, voice_text=full)
        self.save()

    @tasks.loop(seconds=5)
    async def role_worker(self):
        if not self.enabled or not self.is_ready():
            return
        if not self.cfg.get("sync_roles"):
            return
        source = self.get_guild(self.cfg["source_guild_id"])
        if source is None:
            return
        try:
            await self.sync_role_roster(source)
        except discord.HTTPException as exc:
            LOG.warning("Không cập nhật được bảng role: %s; sẽ thử lại.", exc)

    @role_worker.before_loop
    async def before_role_worker(self):
        await self.wait_until_ready()

    @tasks.loop(seconds=3)
    async def worker(self):
        if not self.enabled or not self.is_ready():
            return
        self.tick += 1
        if self.tick % 20 == 0:
            self.dirty.update(self.selected)
        source = self.get_guild(self.cfg["source_guild_id"])
        if source is None:
            return
        self.discover(source)
        pending = self.dirty.copy()
        self.dirty.difference_update(pending)
        for cid in pending & self.selected:
            src = source.get_channel(cid)
            if src is None:
                continue
            try:
                dst, record = await self.destination(src)
                await self.sync_messages(src, dst, record)
                if isinstance(src, discord.VoiceChannel):
                    await self.sync_voice(src, dst, record)
            except discord.HTTPException as exc:
                LOG.warning("Kênh %s: %s; sẽ thử lại.", cid, exc)
                self.dirty.add(cid)

    @worker.before_loop
    async def before_worker(self):
        await self.wait_until_ready()

    async def on_interaction(self, interaction):
        return

    async def on_message(self, message):
        if not message.guild or message.guild.id != self.cfg["source_guild_id"]:
            return
        if message.channel.id in self.selected:
            self.dirty.add(message.channel.id)

    async def on_member_update(self, before, after):
        if after.guild.id == self.cfg["source_guild_id"] and before.roles != after.roles:
            if self.enabled and self.cfg.get("sync_roles"):
                try:
                    await self.sync_role_roster(after.guild)
                except discord.HTTPException:
                    pass

    async def on_raw_message_edit(self, payload):
        if payload.channel_id in self.selected:
            self.dirty.add(payload.channel_id)

    async def on_raw_message_delete(self, payload):
        if payload.channel_id in self.selected:
            await self.mark_message_deleted(payload.channel_id, payload.message_id)

    async def on_raw_bulk_message_delete(self, payload):
        if payload.channel_id in self.selected:
            for mid in payload.message_ids:
                await self.mark_message_deleted(payload.channel_id, mid)

    async def on_voice_state_update(self, member, before, after):
        for channel in (before.channel, after.channel):
            if channel and channel.id in self.selected:
                self.dirty.add(channel.id)


def main():
    load_dotenv(ROOT / ".env")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    token = os.getenv("DISCORD_BOT_TOKEN", "")
    if not token or token == "PASTE_BOT_TOKEN_HERE":
        raise SystemExit("Điền token BOT vào .env (không dùng token tài khoản cá nhân).")
    Mirror(read_config()).run(token, log_handler=None)


if __name__ == "__main__":
    main()