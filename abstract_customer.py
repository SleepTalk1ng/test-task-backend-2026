import json
import logging
from collections import defaultdict
from typing import Optional, Dict, List

from sqlalchemy import text
from sqlalchemy.orm.attributes import flag_modified

from amocrm import Customer, Lead
from models import AbstractCustomer, db, AbcCustomerEntities

logger = logging.getLogger(__name__)


def update_jsonb_field(entity_id: int,
                       attribute: AbcCustomerEntities,
                       index: int, new_value: Dict,
                       table_name='abstract_customer'):
    """Обновление jsonb поля в указанной таблице."""
    if table_name != 'abstract_customer':
        raise Exception("Неизвестная таблица")
    query = text(f"""
        UPDATE {table_name}
        SET {attribute.value} = jsonb_set(
            COALESCE({attribute.value}, '[]'::jsonb),
            CAST(:path AS text[]),
            CAST(:value AS jsonb),
            true
        )
        WHERE id = :entity_id
    """)
    db.session.execute(query, {
        'path': f'{{{index}}}',
        'value': json.dumps(new_value),
        'entity_id': entity_id
    })


def dict_compare_merge(l_dict1: Optional[List[ dict ]], l_dict2: Optional[List[ dict ]]) -> List[dict]:
    d = defaultdict(dict)
    for i in (l_dict1 or []) + (l_dict2 or []):
        entity_id = i.get('general', {}).get('id')
        if entity_id is not None:
            d[entity_id].update(i)

    return list(d.values())


def get_customer_json(customer: Customer) -> dict:
    cf = customer.custom_fields_values or {}
    return {
        'general': {
            'id': customer.id,
            'login': customer.get_field('Логин'),
            'billing_type': customer.get_field('Вид оплаты'),
            'is_robot': bool(cf.get('Робот')),
            'is_nds': bool(cf.get('Оплата с НДС')),
            'system': customer.get_field('Система'),
            'host': customer.get_field('Хост'),
            'name': customer.name,
        },
        'api_value': customer.as_dict()
    }


def get_lead_json(lead: Lead) -> dict:
    return {
        'general': {
            'id': lead.id,
            'login': lead.get_field('Логин'),
            'name': lead.name,
        },
        'api_value': lead.as_dict()
    }


def upsert_json_entity(items: Optional[List[dict]], new_item: dict) -> List[dict]:
    """Обновляет элемент в списке по general.id или добавляет его в конец."""
    if not items:
        return [new_item]
    target_id = new_item['general']['id']
    updated = list(items)
    for idx, current in enumerate(updated):
        if current.get('general', {}).get('id') == target_id:
            updated[idx] = new_item
            return updated
    updated.append(new_item)
    return updated


def get_abccustomer_by_lead_login(lead_login: str) -> Optional[AbstractCustomer]:
    if not lead_login:
        return None
    customers = db.session.query(AbstractCustomer).filter(
        AbstractCustomer.amo_leads.op('@>')([{'general': {'login': lead_login}}])
    ).limit(2).all()

    if len(customers) > 1:
        logger.warning(f"Lead {lead_login} has more than one abccustomer")
        return None
    if not customers:
        return None

    return customers[0]


def get_abccustomer_by_amocustomer_login(customer_login: str) -> Optional[AbstractCustomer]:
    if not customer_login:
        return None
    customers = db.session.query(AbstractCustomer).filter(
        AbstractCustomer.amo_customers.op('@>')([{'general': {'login': customer_login}}])
    ).limit(2).all()

    if len(customers) > 1:
        logger.warning(f"Customer {customer_login} has more than one abccustomer")
        return None
    if not customers:
        return None

    return customers[0]


def create_or_update_customer(abstract_customer: Optional[AbstractCustomer],
                              amo_customer: Customer) -> AbstractCustomer:
    customer_json = get_customer_json(amo_customer)
    if abstract_customer is None:
        new_abstract_customer = AbstractCustomer(
            amo_customers=[customer_json],
        )
        db.session.add(new_abstract_customer)
        return new_abstract_customer

    abstract_customer.amo_customers = upsert_json_entity(abstract_customer.amo_customers, customer_json)
    flag_modified(abstract_customer, "amo_customers")
    return abstract_customer


def create_or_update_lead(abc_customer: Optional[AbstractCustomer],
                          amo_lead: Lead,
                          commit: bool = True) -> AbstractCustomer:
    lead_json = get_lead_json(amo_lead)
    if abc_customer is None:
        new_abstract_customer = AbstractCustomer(
            amo_leads=[lead_json]
        )
        db.session.add(new_abstract_customer)
        if commit:
            db.session.commit()
        return new_abstract_customer

    abc_customer.amo_leads = upsert_json_entity(abc_customer.amo_leads, lead_json)
    flag_modified(abc_customer, "amo_leads")
    if commit:
        db.session.commit()
    return abc_customer