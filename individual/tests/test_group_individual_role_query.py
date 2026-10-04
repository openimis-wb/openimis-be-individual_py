import json

from core.models.openimis_graphql_test_case import BaseTestContext, openIMISGraphQLTestCase
from core.test_helpers import create_admin_role, create_test_interactive_user
from individual.models import GroupIndividual
from individual.tests.test_helpers import (
    add_individual_to_group,
    create_group_with_individual,
    create_individual,
)


class GroupIndividualRoleQueryTest(openIMISGraphQLTestCase):
    """A stored role outside GroupIndividual.Role resolves to null instead of
    failing the whole list; the field keeps its enum type."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.admin_user = create_test_interactive_user(
            username="roleQueryAdmin", roles=[create_admin_role().id])
        cls.admin_token = BaseTestContext(user=cls.admin_user).get_jwt()
        username = cls.admin_user.username
        cls.head, cls.group, cls.head_membership = create_group_with_individual(username)
        cls.members = {}
        for stored_role in ('', 'PETITFILS', None):
            individual = create_individual(username)
            membership = add_individual_to_group(username, individual, cls.group, is_head=False)
            GroupIndividual.objects.filter(id=membership.id).update(role=stored_role)
            cls.members[stored_role] = individual

    def _query(self, field, filter_arg):
        query_str = f'''query {{
          {field}({filter_arg}) {{
            totalCount
            edges {{ node {{ individual {{ uuid }} role }} }}
          }}
        }}'''
        return self.query(query_str, headers={"HTTP_AUTHORIZATION": f"Bearer {self.admin_token}"})

    def _roles_by_individual(self, response, field):
        self.assertResponseNoErrors(response)
        edges = json.loads(response.content)['data'][field]['edges']
        return {e['node']['individual']['uuid']: e['node']['role'] for e in edges}

    def test_members_with_a_role_outside_the_enum_are_listed_with_a_null_role(self):
        response = self._query('groupIndividual', f'group_Id: "{self.group.uuid}"')

        roles = self._roles_by_individual(response, 'groupIndividual')

        self.assertEqual(roles, {
            str(self.head.uuid): 'HEAD',
            str(self.members[''].uuid): None,
            str(self.members['PETITFILS'].uuid): None,
            str(self.members[None].uuid): None,
        })

    def test_history_rows_with_a_role_outside_the_enum_resolve_to_null(self):
        username = self.admin_user.username
        individual = create_individual(username)
        membership = add_individual_to_group(username, individual, self.group, is_head=False)
        membership.role = 'GENDRE'
        membership.save(username=username)

        response = self._query('groupIndividualHistory', f'individual_Id: "{individual.uuid}"')

        self.assertResponseNoErrors(response)
        edges = json.loads(response.content)['data']['groupIndividualHistory']['edges']
        self.assertEqual([e['node']['role'] for e in edges], [None, None])

    def test_role_keeps_its_enum_type(self):
        query_str = '''query {
          groupIndividual: __type(name: "GroupIndividualGQLType") {
            fields { name type { name kind } }
          }
          history: __type(name: "GroupIndividualHistoryGQLType") {
            fields { name type { name kind } }
          }
        }'''
        response = self.query(query_str, headers={"HTTP_AUTHORIZATION": f"Bearer {self.admin_token}"})
        self.assertResponseNoErrors(response)
        data = json.loads(response.content)['data']

        for type_name, enum_name in (('groupIndividual', 'GroupIndividualRole'),
                                     ('history', 'HistoricalGroupIndividualRole')):
            role = next(f for f in data[type_name]['fields'] if f['name'] == 'role')
            self.assertEqual(role['type'], {'name': enum_name, 'kind': 'ENUM'})
