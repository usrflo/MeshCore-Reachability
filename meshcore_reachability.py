#!/usr/bin/env python3
import asyncio
import argparse
import threading
import time
from meshcore import MeshCore
from meshcore.events import EventType
from meshcoredecoder import MeshCoreDecoder
from meshcoredecoder.types.enums import PayloadType
from meshcoredecoder.utils.enum_names import (
    get_route_type_name,
    get_payload_type_name,
    get_device_role_name,
)
import sqlite3
import os
from datetime import datetime
import json
import random
from urllib.parse import urlencode
import base64
import io

import qrcode


# --- DB-Helferfunktionen -------------------------------------------------


def init_db(db_path: str):
    """Initialisiert das neue Reachability-Datenmodell (nodes, paths, traces)."""
    init_db_needed = not os.path.exists(db_path)
    conn = sqlite3.connect(db_path, check_same_thread=False)
    if init_db_needed:
        c = conn.cursor()
        # saves nodes from adverts
        c.execute(
            """CREATE TABLE IF NOT EXISTS nodes (
            public_key CHAR(64) PRIMARY KEY,
            name VARCHAR(50),
            role VARCHAR(5),
            latitude REAL,
            longitude REAL,
            lastpath TEXT,
            lastmod TEXT
        )"""
        )
        # saves partial and full paths from adverts if a trace was executed
        c.execute(
            """CREATE TABLE IF NOT EXISTS paths (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            path TEXT,
            target_node TEXT,
            count INTEGER DEFAULT 1,
            lastmod TEXT
        )"""
        )
        # saves snr results of trace executions; snr_values is null if trace did not succeed
        c.execute(
            """CREATE TABLE IF NOT EXISTS traces (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            path_id INTEGER,
            timestamp TEXT,
            snr_values TEXT,
            FOREIGN KEY(path_id) REFERENCES paths(id)
        )"""
        )
        # create view
        c.execute(
            """CREATE VIEW IF NOT EXISTS pathtraces AS
                SELECT
                    p.id         AS path_id,
                    t.id         AS trace_id,
                    p.path,
                    p.count,
                    t.snr_values,
                    COALESCE(
                        (
                            SELECT n.name
                            FROM nodes AS n
                            WHERE n.public_key = p.target_node
                        ),
                        (
                            SELECT GROUP_CONCAT(n.name, ', ')
                            FROM nodes AS n
                            WHERE SUBSTR(n.public_key, 1, 2)
                                = SUBSTR(p.target_node, 1, 2)
                        )
                        ) AS node_name,
                    t.timestamp
                FROM paths AS p
                JOIN traces AS t
                    ON p.id = t.path_id;
            """
        )
        conn.commit()
    return conn

def formatPath(path_elems: list, default=""):
    if not path_elems:
        return default
    return ",".join(path_elems)

def write_node_to_db(conn: sqlite3.Connection, public_key, name, role, latitude, longitude, lastpath):
    """Schreibt/aktualisiert einen Node-Eintrag in der Tabelle 'nodes'."""
    c = conn.cursor()
    timestamp = datetime.now().isoformat(" ", "seconds")
    c.execute("SELECT public_key FROM nodes WHERE public_key = ?", (public_key,))
    result = c.fetchone()
    if result is None:
        c.execute(
            "INSERT INTO nodes (public_key, name, role, latitude, longitude, lastpath, lastmod) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (public_key, name, role, latitude, longitude, lastpath, timestamp),
        )
    else:
        update_fields = []
        update_values = []
        if name is not None:
            update_fields.append("name = ?")
            update_values.append(name)
        if role is not None:
            update_fields.append("role = ?")
            update_values.append(role)
        if latitude is not None:
            update_fields.append("latitude = ?")
            update_values.append(latitude)
        if longitude is not None:
            update_fields.append("longitude = ?")
            update_values.append(longitude)
        if lastpath is not None:
            update_fields.append("lastpath = ?")
            update_values.append(lastpath)
        update_fields.append("lastmod = ?")
        update_values.append(timestamp)
        update_values.append(public_key)
        sql = "UPDATE nodes SET " + ", ".join(update_fields) + " WHERE public_key = ?"
        c.execute(sql, tuple(update_values))
    conn.commit()


# --- Kombinierter Thread: Adverts sammeln und Pfade verarbeiten ----------


def advert_and_path_thread(port: str, db_path: str, stop_event: threading.Event):
    """Sammelt MeshCore-Adverts und testet die Erreichbarkeit der Quelle über send_msg und ACK"""

    async def _run():
        mc: MeshCore | None = None
        subscription = None

        async def handle_rf_packet(event):
            nonlocal mc, subscription

            packet = event.payload
            if isinstance(packet, dict) and "payload" in packet:
                packet = MeshCoreDecoder.decode(packet["payload"])
                # print(f"  Route Type: {get_route_type_name(packet.route_type)}")
                # print(f"  Payload Type: {get_payload_type_name(packet.payload_type)}")
                # print(f"  Message Hash: {packet.message_hash}")
                # print(f"  Message Path: {packet.path}")

                if packet.payload_type == PayloadType.Advert and packet.payload.get("decoded"):
                    
                    # detach from further events to focus on path tracing            
                    mc.unsubscribe(subscription)
                    
                    advert = packet.payload["decoded"]
                    name = advert.app_data.get("name")
                    roleval = advert.app_data.get("device_role")
                    role = get_device_role_name(roleval) if roleval is not None else None
                    latitude = None
                    longitude = None
                    print("")
                    print(f"Received Advert from {role} {name}, Pubkey-Prefix: {advert.public_key[:8]} via Path {formatPath(packet.path, "<direct>")}")
                    if advert.app_data.get("location"):
                        location = advert.app_data["location"]
                        latitude = location.get("latitude")
                        longitude = location.get("longitude")
                        # print(f"  Location: {latitude}, {longitude}")
                    write_node_to_db(conn, advert.public_key, name, role, latitude, longitude, formatPath(packet.path))
                    #full_path = [advert.public_key]
                    #if packet.path:
                    #    full_path += packet.path
                    # if role == "Chat Node":
                    await process_advert(role, advert.public_key, packet.path)
                    # now listen to RX log events again
                    subscription = mc.subscribe(EventType.RX_LOG_DATA, handle_rf_packet)
                else:
                    print(".", end='')
                    
        async def process_advert(role, public_key, path):
            nonlocal mc

            try:
                
                # examine the path
                
                path_elems = []
                if path:
                    path_elems = list(reversed(path))
                    
                if role != "Chat Node":
                    path_elems += [public_key]

                prefix = []
                prefix_short = []
                for elem in path_elems:
                    prefix.append(elem)
                    prefix_short.append(elem[:2])
                    path_id, now_ts = _ensure_path_record(conn, prefix)

                    needsNewTrace, lastTraceFailed = _needs_new_trace(conn, path_id, now_ts)
                    if not needsNewTrace and lastTraceFailed:
                        # the prefix path could not be reached last time, so stop the whole process
                        return

                    if needsNewTrace:                        
                        if len(prefix_short) == 1:
                            full_path = list(prefix_short)
                        else:
                            full_path = list(prefix_short) + list(reversed(prefix_short[:-1]))

                        full_path_flat = formatPath(full_path)
                        snr_values = await _execute_trace_for_path_async(mc, full_path_flat)
                        if not snr_values:
                            print("Second trace attempt:")
                            snr_values = await _execute_trace_for_path_async(mc, full_path_flat)
                        _insert_trace_result(conn, path_id, snr_values)
                        if not snr_values:
                            # the prefix path could not be reached, so stop the whole process
                            return
                
                if role != "Chat Node":
                    return
                
                # try to send a message to a chat node with confirmation as the path to the node was traceable
                
                assumed_out_path = []
                full_path = [public_key]
                if path:
                    assumed_out_path = list(reversed(path))
                    full_path = assumed_out_path + [public_key]

                # check if path was checked already
                path_id, now_ts = _ensure_path_record(conn, full_path)

                needsNewTrace, lastTraceFailed = _needs_new_trace(conn, path_id, now_ts)

                if not needsNewTrace:
                    return

                # try to contact Chat Node
                contact = mc.get_contact_by_key_prefix(public_key)
                if contact is None:
                    # refresh the list
                    await mc.commands.get_contacts()
                    contact = mc.get_contact_by_key_prefix(public_key)
                    if contact is None:
                        print("Your contact list seems to be filled totally; you need to remove unrequired records to process new Chat Node adverts")
                        return

                saved_out_path = contact["out_path"]

                # change out path to assumed out path from advert
                if not assumed_out_path:
                    op_res = await mc.commands.reset_path(contact)
                    if op_res.type == EventType.ERROR:
                        return
                else:
                    op_res = await mc.commands.change_contact_path(contact, path="".join(assumed_out_path))
                    if op_res.type == EventType.ERROR:
                        return
                
                # send back a message and wait for the confirmation
                send_event = await mc.commands.send_msg(dst=contact, msg="Received your advert, testing reverse connection")
                if send_event.type == EventType.ERROR:
                    print(f"Error {send_event}")
                    await mc.commands.change_contact_path(contact, path=saved_out_path)
                    return
                
                # Wait for ACK
                exp_ack = send_event.payload["expected_ack"].hex()
                timeout = 0
                min_timeout = 8
                timeout = send_event.payload["suggested_timeout"] / 1000 * 1.2 if timeout==0 else timeout
                timeout = timeout if timeout > min_timeout else min_timeout
                send_resp_event = await mc.wait_for_event(EventType.ACK, attribute_filters={"code": exp_ack}, timeout=timeout)
                if send_resp_event:
                    print(f"Reverse message to {contact["adv_name"]} was confirmed (ACK)")
                    _insert_trace_result(conn, path_id, "ACK")
                else:
                    print(f"Reverse message to {contact["adv_name"]} failed")
                    # reset path to previous setting
                    if not saved_out_path:
                        await mc.commands.reset_path(contact)
                    else:
                        await mc.commands.change_contact_path(contact, path=saved_out_path)

            except Exception as e:
                print(f"[process_advert] Error: {e}")

            finally:
                 await asyncio.sleep(0.1)

        try:
            nonlocal_mc: MeshCore | None  # type: ignore[unused-ignore]
            conn = init_db(db_path)

            print(f"[collector] Connecting to {port}...")
            mc = await MeshCore.create_serial(port, 115200)
            
            # forget all repeaters that were not updated in the last 2 days to provide room for companions
            days_ago_ts = int(time.time() - 2 * 24 * 60 * 60)
            
            # load contacts and cleanup repeaters
            print(f"[collector] Fetch contacts and cleanup repeaters")
            contacts = await mc.commands.get_contacts()
            if contacts and contacts.payload:
                for contact in contacts.payload:
                    if contacts.payload[contact]["type"]==2 and contacts.payload[contact]["lastmod"]<days_ago_ts:
                        await mc.commands.remove_contact(contact)

            # last_processed_ts = datetime.now().isoformat(" ", "seconds")
            print("[collector] Waiting for log data")
            
            subscription = mc.subscribe(EventType.RX_LOG_DATA, handle_rf_packet)

            while not stop_event.is_set():
                await asyncio.sleep(2)

        except Exception as e:
            print(f"[collector] Error: {e}")
            
        finally:
            mc.disconnect()
            conn.close()

    asyncio.run(_run())


# --- Thread 2: Auswertung nach 'nodes' geschriebenen Pfaden ----------------

def _ensure_path_record(conn: sqlite3.Connection, path_elements):
    """Sucht oder legt einen Pfad in 'paths' an und erhöht count.

    path_elements ist eine Liste von Kürzeln, z.B. ['92'] oder ['92','c0'].
    Gibt (path_id, now_ts) zurück.
    """
    c = conn.cursor()
    now_ts = datetime.now().isoformat(" ", "seconds")
    path_str = formatPath(path_elements)
    c.execute("SELECT id, count FROM paths WHERE path = ?", (path_str,))
    row = c.fetchone()
    if row:
        pid, cnt = row
        cnt_new = cnt + 1
        c.execute(
            "UPDATE paths SET count = ?, lastmod = ? WHERE id = ?",
            (cnt_new, now_ts, pid),
        )
    else:
        c.execute(
            "INSERT INTO paths (path, target_node, count, lastmod) VALUES (?, ?, ?, ?)",
            (path_str, path_elements[-1], 1, now_ts),
        )
        pid = c.lastrowid
    conn.commit()
    return pid, now_ts

def _needs_new_trace(conn: sqlite3.Connection, path_id: int, now_ts: str) -> tuple[bool,bool]:
    """Prüft, ob für path_id ein neuer Trace ausgeführt werden soll.

    1. Return Value:
    - Wenn kein Trace-Eintrag existiert -> True
    - Wenn letzter Trace älter als 24 Stunden -> True
    2. Return Value:
    - Falls der letzte Trace fehlgeschlagen ist --> True
    """
    c = conn.cursor()
    c.execute(
        "SELECT timestamp, snr_values FROM traces WHERE path_id = ? ORDER BY id DESC LIMIT 1",
        (path_id,),
    )
    row = c.fetchone()
    if not row:
        return True,False
    last_ts = datetime.fromisoformat(row[0])
    now_dt = datetime.fromisoformat(now_ts)
    delta = now_dt - last_ts
    return delta.total_seconds() > 3600*24, row[1] is None # > 24 Stunden


async def _execute_trace_for_path_async(mc: MeshCore, full_path):
    """Führt den Trace asynchron aus und gibt JSON-Array der SNR-Werte zurück oder None."""
    await asyncio.sleep(1)

    try:
        import random

        tag = random.randint(1, 0xFFFFFFFF)
        result = await mc.commands.send_trace(path=full_path, tag=tag)

        if result.type == EventType.ERROR:
            print(f"Failed to send trace packet with path={full_path}: {result.payload.get('reason', 'unknown error')}")
            return None
        if result.type != EventType.MSG_SENT:
            print("Failed to send trace packet with path={full_path}")
            return None

        print(f"Sent trace packet with path={full_path} and tag={tag}, waiting for response ...")
        
        event = await mc.wait_for_event(
            EventType.TRACE_DATA,
            attribute_filters={"tag": tag},
            timeout=15
        )
        if not event:
            print(f"No trace response received with path={full_path} and tag={tag} within timeout")
            return None

        trace = event.payload
        print(f"Trace data received for path={full_path} and tag={tag}:")
        # print(f"  Tag: {trace['tag']}")
        # print(f"  Flags: {trace.get('flags', 0)}")
        # print(f"  Path Length: {trace.get('path_len', 0)}")

        # FSTODO
        if not trace.get("path"):
            return None

        snr_list = []
        for node in trace["path"]:
            if "snr" not in node:
                return None
            # TODO
            print(f"  {node}")
            snr_list.append(str(node["snr"]))
        
        return ','.join(snr_list)

    except Exception as e:
        print(f"Trace exception: {e}")
        return None


def _insert_trace_result(conn: sqlite3.Connection, path_id: int, snr_values: str | None):
    c = conn.cursor()
    now_ts = datetime.now().isoformat(" ", "seconds")
    c.execute(
        "INSERT INTO traces (path_id, timestamp, snr_values) VALUES (?, ?, ?)",
        (path_id, now_ts, snr_values),
    )
    conn.commit()


# --- Thread 3: Visualisierung (Dash + Cytoscape) --------------------------


def create_dash_app_from_db(db_path, maptiler_api_key: str | None = None):
    import dash
    from dash import html, dcc, Output, Input, State
    import dash_leaflet as dl
    import sqlite3

    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    c.execute("""SELECT 
        n.public_key, 
        n.name, 
        n.role, 
        n.latitude, 
        n.longitude, 
        n.lastpath, 
        n.lastmod, 
        EXISTS(
            SELECT 1 
            FROM paths AS p
            JOIN traces AS t ON t.path_id = p.id
            WHERE p.target_node = n.public_key
            AND t.snr_values IS NOT NULL
        ) AS reachable
        FROM nodes AS n""")
    rows = c.fetchall()

    node_meta = {}
    for row in rows:
        node_meta[row[0]] = {
            "public_key": row[0],
            "name": row[1],
            "role": row[2],
            "latitude": row[3],
            "longitude": row[4],
            "lastpath": row[5],
            "lastmod": row[6],
            "reachable": row[7],
        }

    def node_label(n):
        return n[:4] if len(n) == 64 else n

    def val_ok(val):
        return val not in (None, 0, 0.0)

    coords = [
        (meta["longitude"], meta["latitude"])
        for meta in node_meta.values()
        if val_ok(meta["longitude"]) and val_ok(meta["latitude"])
    ]

    if coords:
        min_lon = min(c[0] for c in coords)
        max_lon = max(c[0] for c in coords)
        min_lat = min(c[1] for c in coords)
        max_lat = max(c[1] for c in coords)
        width = max_lon - min_lon or 1
        height = max_lat - min_lat or 1
    else:
        min_lon = max_lon = min_lat = max_lat = width = height = 1

    IMG_W, IMG_H = 1000, 800

    def map_coords_to_latlon(lon, lat):
        return [lat, lon]

    # Leaflet-Kartenmitte bestimmen
    if coords:
        center_lat = (min_lat + max_lat) / 2
        center_lon = (min_lon + max_lon) / 2
        zoom = 8
    else:
        center_lat = 49.2125578
        center_lon = 16.62662018
        zoom = 8

    app = dash.Dash(__name__)
    app.layout = html.Div(
        [
            html.H2("MeshCore Reachability"),
            dcc.Checklist(
                id="reachable-filter",
                # : bidirectional, checked latest path from adverts
                options=[{"label": "show reachable nodes only", "value": "reachable_only"}],
                value=[],
                style={"margin": "8px 0"},
            ),
            dl.Map(
                center=[center_lat, center_lon],
                zoom=zoom,
                style={"width": "100%", "height": "800px", "position": "relative"},
                children=[
                    dl.TileLayer(
                        url=(
                            f"https://api.maptiler.com/maps/topo-v4/{{z}}/{{x}}/{{y}}.png?key={maptiler_api_key}"
                            if maptiler_api_key
                            else "https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
                        ),
                        attribution=(
                            "<a href='https://www.maptiler.com/copyright/' target='_blank'>&copy; MapTiler</a> "
                            "<a href='https://www.openstreetmap.org/copyright' target='_blank'>&copy; OpenStreetMap contributors</a>"
                            if maptiler_api_key
                            else "&copy; OpenStreetMap contributors"
                        ),
                        tileSize=512 if maptiler_api_key else 256,
                        zoomOffset=-1 if maptiler_api_key else 0,
                    ),
                    dl.LayerGroup(id="node-layer"),
                ],
            ),
            html.Div(
                id="node-details-overlay",
                style={
                    "margin": "16px",
                    "padding": "8px",
                    "border": "1px solid #ccc",
                    "display": "none",
                    "background": "#fafafa",
                    "white-space": "pre",
                },
            ),
            dcc.Store(id="node-meta-store", data=node_meta),
        ]
    )

    @app.callback(
        Output("node-layer", "children"),
        Input("reachable-filter", "value"),
        State("node-meta-store", "data"),
    )
    def update_node_markers(filter_values, node_meta_store):
        show_reachable_only = "reachable_only" in (filter_values or [])

        def val_ok(val):
            return val not in (None, 0, 0.0)

        markers = []
        for meta in node_meta_store.values():
            if show_reachable_only and meta.get("reachable", 0) == 0:
                continue
            if val_ok(meta.get("longitude")) and val_ok(meta.get("latitude")):
                
                role_id=1
                if meta.get("role")=="Repeater":
                    role_id=2
                elif meta.get("role")=="Room Server":
                    role_id=3

                mclink = f"meshcore://contact/add?{urlencode({"name": meta.get("name")})}&public_key={meta["public_key"]}&type={role_id}"

                # QR-Code für mclink erzeugen (lokal, ohne externen Dienst)
                qr = qrcode.QRCode(box_size=4, border=2)
                qr.add_data(mclink)
                qr.make(fit=True)
                img = qr.make_image(fill_color="black", back_color="white")
                buf = io.BytesIO()
                img.save(buf, format="PNG")
                img_b64 = base64.b64encode(buf.getvalue()).decode("ascii")
                mclink_qr_data_url = f"data:image/png;base64,{img_b64}"

                markers.append(
                    dl.Marker(
                        id=f"node-marker-{meta["public_key"]}",
                        position=map_coords_to_latlon(meta["longitude"], meta["latitude"]),
                        children=dl.Tooltip(
                            content=f"<b>{meta['role']} {meta['name']}</b><br/>Latest Path: {meta["lastpath"] or "[direct]"}<br/>Public-Key: {meta["public_key"][:10]}...<br/><img src='{mclink_qr_data_url}' alt='MC-Link QR Code'/>"
                        ),
                        icon={
                            "iconUrl": dash.get_asset_url(f"{meta["role"].lower().replace(' ', '-')}{"_reachable" if meta["reachable"]==1 else ""}.svg"),
                            "iconSize": "24",
                            "shadowUrl": dash.get_asset_url("iconbg.svg"),
                            "shadowSize": "28"
                        }
                    )
                )
        return markers


    return app


def dash_server_thread(db_path: str, stop_event: threading.Event, maptiler_api_key: str | None = None):
    """Startet die Dash-Anwendung (blockierend in diesem Thread)."""
    app = create_dash_app_from_db(db_path, maptiler_api_key=maptiler_api_key)
    # Dash selbst hat keine eingebaute Möglichkeit, über ein Event sauber zu stoppen.
    # Wir starten den Server einfach und verlassen uns auf Prozessende.
    print("[dash] Starting Dash server on http://0.0.0.0:5342 ...")
    app.run(host="0.0.0.0", port=5342, debug=False)


# --- main() -------


async def main():
    """Hier wird nur noch die Thread-Orchestrierung übernommen.

    Erwartet, dass Argumente (Port, DB-Pfad) bereits wie gewünscht definiert sind.
    """
    parser = argparse.ArgumentParser(description="MeshCore Reachability Graph")
    parser.add_argument("-p", "--port", required=True, help="LoRa-Device serial port")
    parser.add_argument("--db", default="mcreach.sqlite", help="SQLite database file")
    parser.add_argument("-ak", "--maptiler_api_key", dest="maptiler_api_key", help="Optional MapTiler API key for background map", required=False)
    args = parser.parse_args()

    db_path = args.db
    maptiler_api_key = args.maptiler_api_key
    stop_event = threading.Event()

    # Kombinierter Thread: Adverts einsammeln, Pfade auswerten und Traces sequenziell ausführen
    t_collect_paths = threading.Thread(
        target=advert_and_path_thread,
        args=(args.port, db_path, stop_event),
        daemon=True,
    )

    t_collect_paths.start()

    print("[main] Collector thread started. Launching Dash app in main process (Ctrl+C to stop)...")

    try:
        dash_server_thread(db_path, stop_event, maptiler_api_key)
    except KeyboardInterrupt:
        print("[main] Stopping ...")
        stop_event.set()
        # kurze Wartezeit für sauberes Beenden des Collector-Threads
        await asyncio.sleep(2)


if __name__ == "__main__":
    import asyncio as _asyncio

    _asyncio.run(main())
