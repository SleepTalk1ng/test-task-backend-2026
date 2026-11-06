import logging
from itertools import islice
from typing import Iterator, List, Dict, Optional, Set, Union, Iterable

from sqlalchemy import delete, select
from sqlalchemy.orm.attributes import flag_modified

from abstract_customer import (
    get_abccustomer_by_lead_login,
    create_or_update_customer,
    get_abccustomer_by_amocustomer_login,
    dict_compare_merge,
    create_or_update_lead,
    get_customer_json,
    get_lead_json,
    upsert_json_entity,
)
from amocrm import Customer, COMPANIES, CONTACTS, Lead, amocrm_get
from models import db, AbstractCustomer

logger = logging.getLogger(__name__)

BATCH_SIZE = 2000


def _has_delete_marker(name: Optional[str]) -> bool:
    return 'удалить' in (name or '').lower()


def _chunked(iterable: Iterable, size: int) -> Iterator[list]:
    iterator = iter(iterable)
    while True:
        chunk = list(islice(iterator, size))
        if not chunk:
            break
        yield chunk


class SyncIndex:
    """Компактный индекс в памяти для O(1) сопоставления покупателей и сделок."""

    def __init__(self):
        self.existing_abc_ids: Set[int] = set()
        self.deleted_abc_ids: Set[int] = set()
        self.cust_id_to_abc: Dict[int, int] = {}
        self.lead_id_to_abc: Dict[int, int] = {}
        self.cust_login_to_abcs: Dict[str, Union[int, Set[int]]] = {}
        self.lead_login_to_abcs: Dict[str, Union[int, Set[int]]] = {}

        # Начальные ID вложенных сущностей в БД для определения записей, требующих чистки
        self.db_nested_ids: Dict[int, Dict[str, Set[int]]] = {}
        self.dirty_abc_ids: Set[int] = set()
        self.ids_map: Dict[int, Dict[str, Set[int]]] = {}

    @staticmethod
    def _add_login(mapping: Dict[str, Union[int, Set[int]]], login: Optional[str], abc_id: int):
        if not login:
            return
        current = mapping.get(login)
        if current is None:
            mapping[login] = abc_id
        elif isinstance(current, int):
            if current != abc_id:
                mapping[login] = {current, abc_id}
        else:
            current.add(abc_id)

    @staticmethod
    def _replace_abc_in_logins(mapping: Dict[str, Union[int, Set[int]]], old_abc_id: int, new_abc_id: int):
        for login, val in list(mapping.items()):
            if isinstance(val, int):
                if val == old_abc_id:
                    mapping[login] = new_abc_id
            else:
                if old_abc_id in val:
                    val.discard(old_abc_id)
                    val.add(new_abc_id)
                    if len(val) == 1:
                        mapping[login] = next(iter(val))

    def get_unique_abc_by_lead_login(self, login: Optional[str]) -> Optional[int]:
        if not login:
            return None
        val = self.lead_login_to_abcs.get(login)
        if val is None:
            return None
        if isinstance(val, int):
            return val
        if len(val) == 1:
            return next(iter(val))
        logger.warning(f"Lead {login} has more than one abccustomer")
        return None

    def get_unique_abc_by_customer_login(self, login: Optional[str]) -> Optional[int]:
        if not login:
            return None
        val = self.cust_login_to_abcs.get(login)
        if val is None:
            return None
        if isinstance(val, int):
            return val
        if len(val) == 1:
            return next(iter(val))
        logger.warning(f"Customer {login} has more than one abccustomer")
        return None

    def register_customer(self, abc_id: int, customer_id: int, login: Optional[str], name: Optional[str]):
        self.cust_id_to_abc[customer_id] = abc_id
        self._add_login(self.cust_login_to_abcs, login, abc_id)
        if _has_delete_marker(name):
            self.dirty_abc_ids.add(abc_id)

    def register_lead(self, abc_id: int, lead_id: int, login: Optional[str], name: Optional[str]):
        self.lead_id_to_abc[lead_id] = abc_id
        self._add_login(self.lead_login_to_abcs, login, abc_id)
        if _has_delete_marker(name):
            self.dirty_abc_ids.add(abc_id)

    def record_seen_customer(self, abc_id: int, customer: Customer):
        entry = self.ids_map.setdefault(abc_id, {
            "customers": set(),
            "contacts": set(),
            "companies": set(),
            "leads": set(),
        })
        entry["customers"].add(customer.id)
        entry["contacts"].update(c["id"] for c in (customer.contacts or []) if "id" in c)
        entry["companies"].update(c["id"] for c in (customer.companies or []) if "id" in c)

    def record_seen_lead(self, abc_id: int, lead: Lead):
        entry = self.ids_map.setdefault(abc_id, {
            "customers": set(),
            "contacts": set(),
            "companies": set(),
            "leads": set(),
        })
        entry["leads"].add(lead.id)
        entry["contacts"].update(c["id"] for c in (lead.contacts or []) if "id" in c)

    def merge_abc_records(self, target_id: int, source_id: int, source_obj: AbstractCustomer):
        """Переносит все ссылки из удаляемой записи source_id в target_id."""
        for c in (source_obj.amo_customers or []):
            gen = c.get("general", {})
            if gen.get("id") is not None:
                self.cust_id_to_abc[gen["id"]] = target_id
        for l in (source_obj.amo_leads or []):
            gen = l.get("general", {})
            if gen.get("id") is not None:
                self.lead_id_to_abc[gen["id"]] = target_id

        self._replace_abc_in_logins(self.cust_login_to_abcs, source_id, target_id)
        self._replace_abc_in_logins(self.lead_login_to_abcs, source_id, target_id)

        if source_id in self.ids_map:
            src_seen = self.ids_map.pop(source_id)
            dst_seen = self.ids_map.setdefault(target_id, {
                "customers": set(),
                "contacts": set(),
                "companies": set(),
                "leads": set(),
            })
            for key in ("customers", "contacts", "companies", "leads"):
                dst_seen[key].update(src_seen[key])

        self.db_nested_ids.pop(source_id, None)
        self.dirty_abc_ids.discard(source_id)
        self.dirty_abc_ids.add(target_id)
        self.deleted_abc_ids.add(source_id)

    @classmethod
    def load_from_db(cls, session, is_full: bool = True) -> "SyncIndex":
        index = cls()
        stmt = select(
            AbstractCustomer.id,
            AbstractCustomer.amo_customers,
            AbstractCustomer.amo_leads,
            AbstractCustomer.amo_contacts,
            AbstractCustomer.amo_companies,
        ).execution_options(yield_per=BATCH_SIZE)

        for row in session.execute(stmt):
            abc_id = row.id
            index.existing_abc_ids.add(abc_id)

            cust_ids, lead_ids, cont_ids, comp_ids = set(), set(), set(), set()
            has_delete = False

            for c in (row.amo_customers or []):
                gen = c.get("general", {})
                cid = gen.get("id")
                if cid is not None:
                    cust_ids.add(cid)
                    index.cust_id_to_abc[cid] = abc_id
                index._add_login(index.cust_login_to_abcs, gen.get("login"), abc_id)
                if _has_delete_marker(gen.get("name")):
                    has_delete = True

            for l in (row.amo_leads or []):
                gen = l.get("general", {})
                lid = gen.get("login")
                lead_id = gen.get("id")
                if lead_id is not None:
                    lead_ids.add(lead_id)
                    index.lead_id_to_abc[lead_id] = abc_id
                index._add_login(index.lead_login_to_abcs, lid, abc_id)
                if _has_delete_marker(gen.get("name")):
                    has_delete = True

            if is_full:
                for cont in (row.amo_contacts or []):
                    gen = cont.get("general", {})
                    if gen.get("id") is not None:
                        cont_ids.add(gen["id"])
                    if _has_delete_marker(gen.get("name")):
                        has_delete = True

                for comp in (row.amo_companies or []):
                    gen = comp.get("general", {})
                    if gen.get("id") is not None:
                        comp_ids.add(gen["id"])
                    if _has_delete_marker(gen.get("name")):
                        has_delete = True

                index.db_nested_ids[abc_id] = {
                    "customers": cust_ids,
                    "leads": lead_ids,
                    "contacts": cont_ids,
                    "companies": comp_ids,
                }
                if has_delete:
                    index.dirty_abc_ids.add(abc_id)

        return index

    def get_all_dirty_abc_ids(self) -> Set[int]:
        """Возвращает ID записей, в которых есть 'удалить' или не пришедшие в выгрузке вложенные сущности."""
        dirty = set(self.dirty_abc_ids) & set(self.ids_map.keys())
        for abc_id, seen in self.ids_map.items():
            if abc_id in dirty:
                continue
            db_nested = self.db_nested_ids.get(abc_id)
            if not db_nested:
                continue
            if (
                not db_nested["customers"].issubset(seen["customers"])
                or not db_nested["leads"].issubset(seen["leads"])
                or not db_nested["contacts"].issubset(seen["contacts"])
                or not db_nested["companies"].issubset(seen["companies"])
            ):
                dirty.add(abc_id)
        return dirty


def _merge_abstract_customers_in_memory(target: AbstractCustomer, source: AbstractCustomer):
    target.amo_customers = dict_compare_merge(target.amo_customers or [], source.amo_customers or [])
    target.amo_leads = dict_compare_merge(target.amo_leads or [], source.amo_leads or [])
    target.amo_contacts = dict_compare_merge(target.amo_contacts or [], source.amo_contacts or [])
    target.amo_companies = dict_compare_merge(target.amo_companies or [], source.amo_companies or [])
    flag_modified(target, "amo_customers")
    flag_modified(target, "amo_leads")
    flag_modified(target, "amo_contacts")
    flag_modified(target, "amo_companies")


def lead_action(lead: Lead) -> AbstractCustomer:
    """Обработка одной сделки (сохранена для обратной совместимости и одиночных вызовов)."""
    abstract_customer = db.session.query(AbstractCustomer).filter(
        AbstractCustomer.amo_leads.op('@>')([{'general': {'id': lead.id}}])
    ).first()
    lead_login = lead.get_field('Логин')
    if lead_login:
        abs_customer_login = get_abccustomer_by_amocustomer_login(lead_login)
        if abstract_customer is None:
            abstract_customer = abs_customer_login
        elif abs_customer_login is not None and abs_customer_login.id != abstract_customer.id:
            _merge_abstract_customers_in_memory(abstract_customer, abs_customer_login)
            db.session.delete(abs_customer_login)
            db.session.flush()

    return create_or_update_lead(abstract_customer, lead)


def _process_customer_batch(session, customers_batch: List[Customer], index: SyncIndex, is_full: bool):
    candidate_abc_ids: Set[int] = set()
    for customer in customers_batch:
        abc_id = index.cust_id_to_abc.get(customer.id)
        if abc_id is None:
            abc_id = index.get_unique_abc_by_lead_login(customer.get_field('Логин'))
        if abc_id is not None:
            candidate_abc_ids.add(abc_id)

    abc_cache: Dict[int, AbstractCustomer] = {}
    if candidate_abc_ids:
        existing_objs = session.scalars(
            select(AbstractCustomer).where(AbstractCustomer.id.in_(candidate_abc_ids))
        ).all()
        abc_cache = {obj.id: obj for obj in existing_objs}

    pending_new_by_cust_id: Dict[int, AbstractCustomer] = {}
    pending_new_records: List[ tuple[AbstractCustomer, Customer, str] ] = []

    for customer in customers_batch:
        login = customer.get_field('Логин')
        abc_id = index.cust_id_to_abc.get(customer.id)
        if abc_id is None:
            abc_id = index.get_unique_abc_by_lead_login(login)

        abc_obj = abc_cache.get(abc_id) if abc_id is not None else None
        if abc_obj is not None:
            create_or_update_customer(abc_obj, customer)
            index.register_customer(abc_obj.id, customer.id, login, customer.name)
            if is_full:
                index.record_seen_customer(abc_obj.id, customer)
        elif customer.id in pending_new_by_cust_id:
            abc_obj = pending_new_by_cust_id[customer.id]
            create_or_update_customer(abc_obj, customer)
            pending_new_records.append((abc_obj, customer, login))
        else:
            abc_obj = create_or_update_customer(None, customer)
            pending_new_by_cust_id[customer.id] = abc_obj
            pending_new_records.append((abc_obj, customer, login))

    session.flush()

    for abc_obj, customer, login in pending_new_records:
        index.register_customer(abc_obj.id, customer.id, login, customer.name)
        if is_full:
            index.record_seen_customer(abc_obj.id, customer)

    session.commit()
    session.expunge_all()


def _process_lead_batch(session, leads_batch: List[Lead], index: SyncIndex, is_full: bool):
    candidate_abc_ids: Set[int] = set()
    for lead in leads_batch:
        by_lead = index.lead_id_to_abc.get(lead.id)
        if by_lead is not None:
            candidate_abc_ids.add(by_lead)
        lead_login = lead.get_field('Логин')
        if lead_login:
            by_cust = index.get_unique_abc_by_customer_login(lead_login)
            if by_cust is not None:
                candidate_abc_ids.add(by_cust)

    abc_cache: Dict[int, AbstractCustomer] = {}
    if candidate_abc_ids:
        existing_objs = session.scalars(
            select(AbstractCustomer).where(AbstractCustomer.id.in_(candidate_abc_ids))
        ).all()
        abc_cache = {obj.id: obj for obj in existing_objs}

    pending_new_by_lead_id: Dict[int, AbstractCustomer] = {}
    pending_new_records: List[ tuple[AbstractCustomer, Lead, Optional[str]] ] = []

    for lead in leads_batch:
        lead_login = lead.get_field('Логин')
        by_lead_id = index.lead_id_to_abc.get(lead.id)
        by_cust_login = index.get_unique_abc_by_customer_login(lead_login) if lead_login else None

        abc_lead_obj = abc_cache.get(by_lead_id) if by_lead_id is not None else None
        abc_cust_obj = abc_cache.get(by_cust_login) if by_cust_login is not None else None

        target_obj: Optional[AbstractCustomer] = None

        if abc_lead_obj is not None and abc_cust_obj is not None and abc_lead_obj.id != abc_cust_obj.id:
            _merge_abstract_customers_in_memory(abc_lead_obj, abc_cust_obj)
            index.merge_abc_records(abc_lead_obj.id, abc_cust_obj.id, abc_cust_obj)
            abc_cache.pop(abc_cust_obj.id, None)
            session.delete(abc_cust_obj)
            target_obj = abc_lead_obj
        elif abc_lead_obj is not None:
            target_obj = abc_lead_obj
        elif abc_cust_obj is not None:
            target_obj = abc_cust_obj

        if target_obj is not None:
            create_or_update_lead(target_obj, lead, commit=False)
            index.register_lead(target_obj.id, lead.id, lead_login, lead.name)
            if is_full:
                index.record_seen_lead(target_obj.id, lead)
        elif lead.id in pending_new_by_lead_id:
            target_obj = pending_new_by_lead_id[lead.id]
            create_or_update_lead(target_obj, lead, commit=False)
            pending_new_records.append((target_obj, lead, lead_login))
        else:
            target_obj = create_or_update_lead(None, lead, commit=False)
            pending_new_by_lead_id[lead.id] = target_obj
            pending_new_records.append((target_obj, lead, lead_login))

    session.flush()

    for abc_obj, lead, lead_login in pending_new_records:
        index.register_lead(abc_obj.id, lead.id, lead_login, lead.name)
        if is_full:
            index.record_seen_lead(abc_obj.id, lead)

    session.commit()
    session.expunge_all()


def collect_leads_customers(is_full: bool = True):
    """
    Пакетная синхронизация покупателей и сделок из AmoCRM в PostgreSQL.
    """
    session = db.session
    try:
        index = SyncIndex.load_from_db(session, is_full=is_full)

        customers_iter: Iterator[Customer] = amocrm_get(Customer, _with=f"{COMPANIES},{CONTACTS}")
        logger.info(f"Collected customers {is_full=}")

        valid_customers = (
            c for c in customers_iter
            if not (is_full is False and c.status_id == 111349) and c.get_field('Логин')
        )
        for batch in _chunked(valid_customers, BATCH_SIZE):
            _process_customer_batch(session, batch, index, is_full=is_full)

        del customers_iter

        amo_leads_iter: Iterator[Lead] = amocrm_get(Lead, _with=f"{CONTACTS}")
        logger.info(f"Collected leads {is_full=}")

        valid_leads = (
            l for l in amo_leads_iter
            if not (is_full is False and l.status_id == 143)
        )
        for batch in _chunked(valid_leads, BATCH_SIZE):
            _process_lead_batch(session, batch, index, is_full=is_full)

    except Exception as e:
        session.rollback()
        logger.exception(f"Сбор прервался, удаление не запускается: {e}")
        return

    if is_full is False:
        return

    try:
        logger.info("Чистка покупателей общая")
        unseen_ids = index.existing_abc_ids - set(index.ids_map.keys()) - index.deleted_abc_ids
        for id_chunk in _chunked(unseen_ids, BATCH_SIZE):
            session.execute(delete(AbstractCustomer).where(AbstractCustomer.id.in_(id_chunk)))
        session.commit()

        logger.info("Чистка покупателей на уровне сущностей")
        dirty_ids = index.get_all_dirty_abc_ids()
        for id_chunk in _chunked(dirty_ids, BATCH_SIZE):
            abc_customers = session.scalars(
                select(AbstractCustomer).where(AbstractCustomer.id.in_(id_chunk))
            ).all()
            for abc_customer in abc_customers:
                filter_abc_customer(abc_customer=abc_customer, ids_map=index.ids_map, commit=False)
            session.commit()
            session.expunge_all()
    except Exception as e:
        session.rollback()
        logger.exception(f"Чистка: {e}")


def filter_merge_abstract_customer(abc_customer: AbstractCustomer,
                                   attr: str,
                                   check_ids: Iterable[int],
                                   commit: bool = True):
    entity = getattr(abc_customer, attr)
    if not entity:
        return
    valid_ids = set(check_ids)
    filtered = [
        c for c in entity
        if not _has_delete_marker(c.get('general', {}).get('name'))
        and c.get('general', {}).get('id') in valid_ids
    ]
    if len(filtered) != len(entity):
        setattr(abc_customer, attr, filtered)
        flag_modified(abc_customer, attr)
        if commit:
            db.session.commit()


def filter_abc_customer(abc_customer: AbstractCustomer, ids_map: Dict, commit: bool = True):
    seen = ids_map.get(abc_customer.id, {})
    for attr, key in (
        ("amo_leads", "leads"),
        ("amo_customers", "customers"),
        ("amo_contacts", "contacts"),
        ("amo_companies", "companies"),
    ):
        if getattr(abc_customer, attr):
            try:
                filter_merge_abstract_customer(
                    abc_customer=abc_customer,
                    attr=attr,
                    check_ids=seen.get(key, ()),
                    commit=False,
                )
            except Exception as e:
                logger.exception(e)

    if not abc_customer.amo_leads and not abc_customer.amo_customers:
        db.session.delete(abc_customer)

    if commit:
        db.session.commit()