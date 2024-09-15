import json
from core.models.base_mutation import MutationLog
from core.models.openimis_graphql_test_case import openIMISGraphQLTestCase
from core.test_helpers import create_test_interactive_user
from graphql_jwt.shortcuts import get_token
from location.models import Location
from rest_framework import status
from individual.tests.test_helpers import (
    create_individual,
    create_group_with_individual,
    add_individual_to_group,
    BaseTestContext,
    create_sp_role,
    generate_random_string,
)
from location.test_helpers import create_test_village, assign_user_districts


class IndividualGQLMutationTest(openIMISGraphQLTestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        cls.admin_user = create_test_interactive_user(username="adminSeesEveryone")
        cls.admin_token = get_token(cls.admin_user, BaseTestContext(user=cls.admin_user))

        cls.village_a = create_test_village({
            'name': 'Village A',
            'code': 'ViMA',
        })

        cls.village_b = create_test_village({
            'name': 'Village B',
            'code': 'ViMB'
        })

        cls.sp_role = create_sp_role(cls.admin_user)

        cls.dist_a_user = create_test_interactive_user(
            username="districtUserSeesDistrict", roles=[cls.sp_role.id])
        district_a_code = cls.village_a.parent.parent.code
        assign_user_districts(cls.dist_a_user, ["R1D1", district_a_code])
        cls.dist_a_user_token = get_token(cls.dist_a_user, BaseTestContext(user=cls.dist_a_user))

        cls.dist_b_user = create_test_interactive_user(
            username="districtBUser", roles=[cls.sp_role.id])
        district_b_code = cls.village_b.parent.parent.code
        assign_user_districts(cls.dist_b_user, [district_b_code])
        cls.dist_b_user_token = get_token(cls.dist_b_user, BaseTestContext(user=cls.dist_b_user))

        cls.med_enroll_officer = create_test_interactive_user(
            username="medEONoRight", roles=[1]) # 1 is the med enrollment officer role
        cls.med_enroll_officer_token = get_token(
            cls.med_enroll_officer, BaseTestContext(user=cls.med_enroll_officer))

    # overriding helper method from core to allow errors
    def get_mutation_result(self, mutation_uuid, token, internal=False):
        content = None
        while True:
            # wait for the mutation to be done
            if internal:
                filter_uuid = f""" id: "{mutation_uuid}" """
            else:
                filter_uuid = f""" clientMutationId: "{mutation_uuid}" """

            response = self.query(
                f"""
                {{
                mutationLogs({filter_uuid})
                {{
                pageInfo {{ hasNextPage, hasPreviousPage, startCursor, endCursor}}
                edges
                {{
                    node
                    {{
                        id,status,error,clientMutationId,clientMutationLabel,clientMutationDetails,requestDateTime,jsonExt
                    }}
                }}
                }}
                }}

                """,
                headers={"HTTP_AUTHORIZATION": f"Bearer {token}"},
            )
            return json.loads(response.content)

            time.sleep(1)


    def assert_mutation_error(self, uuid, expected_error):
        mutation_result = self.get_mutation_result(uuid, self.admin_token, internal=True)
        mutation_error = mutation_result['data']['mutationLogs']['edges'][0]['node']['error']
        self.assertTrue(expected_error in mutation_error)

    def assert_mutation_success(self, uuid):
        mutation_result = self.get_mutation_result(uuid, self.admin_token, internal=True)
        mutation_status = mutation_result['data']['mutationLogs']['edges'][0]['node']['status']
        self.assertEqual(mutation_status, MutationLog.SUCCESS)


    def test_create_individual_general_permission(self):
        query_str = f'''
            mutation {{
              createIndividual(
                input: {{
                  firstName: "Alice"
                  lastName: "Foo"
                  dob: "2020-02-20"
                }}
              ) {{
                clientMutationId
                internalId
              }}
            }}
        '''

        # Anonymous User has no permission
        response = self.query(query_str)

        content = json.loads(response.content)
        id = content['data']['createIndividual']['internalId']
        self.assert_mutation_error(id, 'mutation.authentication_required')

        # IMIS admin can do everything
        response = self.query(
            query_str,
            headers={"HTTP_AUTHORIZATION": f"Bearer {self.admin_token}"}
        )
        content = json.loads(response.content)
        id = content['data']['createIndividual']['internalId']
        self.assert_mutation_success(id)

        # Health Enrollment Officier (role=1) has no permission
        response = self.query(
            query_str,
            headers={"HTTP_AUTHORIZATION": f"Bearer {self.med_enroll_officer_token}"}
        )
        content = json.loads(response.content)
        id = content['data']['createIndividual']['internalId']
        self.assert_mutation_error(id, 'mutation.authentication_required')

    def test_create_individual_row_security(self):
        query_str = f'''
            mutation {{
              createIndividual(
                input: {{
                  firstName: "Alice"
                  lastName: "Foo"
                  dob: "2020-02-20"
                  villageId: {self.village_a.id}
                }}
              ) {{
                clientMutationId
                internalId
              }}
            }}
        '''

        # SP officer B cannot create individual for district A
        response = self.query(query_str)
        response = self.query(
            query_str,
            headers={"HTTP_AUTHORIZATION": f"Bearer {self.dist_b_user_token}"}
        )
        self.assertResponseNoErrors(response)

        content = json.loads(response.content)
        id = content['data']['createIndividual']['internalId']
        self.assert_mutation_error(id, 'mutation.authentication_required')

        # SP officer A can create individual for district A
        response = self.query(
            query_str,
            headers={"HTTP_AUTHORIZATION": f"Bearer {self.dist_a_user_token}"}
        )
        content = json.loads(response.content)
        id = content['data']['createIndividual']['internalId']
        self.assert_mutation_success(id)

        # SP officer B can create individual for district B
        response = self.query(
            query_str.replace(
                f'villageId: {self.village_a.id}',
                f'villageId: {self.village_b.id}'
            ), headers={"HTTP_AUTHORIZATION": f"Bearer {self.dist_b_user_token}"}
        )
        content = json.loads(response.content)
        id = content['data']['createIndividual']['internalId']
        self.assert_mutation_success(id)

        # SP officer B can create individual without any district
        response = self.query(
            query_str.replace(f'villageId: {self.village_a.id}', ' '),
            headers={"HTTP_AUTHORIZATION": f"Bearer {self.dist_b_user_token}"}
        )
        content = json.loads(response.content)
        id = content['data']['createIndividual']['internalId']
        self.assert_mutation_success(id)

    def test_update_individual_general_permission(self):
        individual = create_individual(self.admin_user.username)
        query_str = f'''
            mutation {{
              updateIndividual(
                input: {{
                  id: "{individual.id}"
                  firstName: "Bob"
                  lastName: "Bar"
                  dob: "2019-09-19"
                }}
              ) {{
                clientMutationId
                internalId
              }}
            }}
        '''

        # Anonymous User has no permission
        response = self.query(query_str)

        content = json.loads(response.content)
        id = content['data']['updateIndividual']['internalId']
        self.assert_mutation_error(id, 'mutation.authentication_required')

        # IMIS admin can do everything
        response = self.query(
            query_str,
            headers={"HTTP_AUTHORIZATION": f"Bearer {self.admin_token}"}
        )
        content = json.loads(response.content)
        id = content['data']['updateIndividual']['internalId']
        self.assert_mutation_success(id)

        # Health Enrollment Officier (role=1) has no permission
        response = self.query(
            query_str,
            headers={"HTTP_AUTHORIZATION": f"Bearer {self.med_enroll_officer_token}"}
        )
        content = json.loads(response.content)
        id = content['data']['updateIndividual']['internalId']
        self.assert_mutation_error(id, 'mutation.authentication_required')

    def test_update_individual_row_security(self):
        individual_a = create_individual(
            self.admin_user.username,
            payload_override={'village': self.village_a},
        )
        query_str = f'''
            mutation {{
              updateIndividual(
                input: {{
                  id: "{individual_a.id}"
                  firstName: "Bob"
                  lastName: "Foo"
                  dob: "2020-02-19"
                }}
              ) {{
                clientMutationId
                internalId
              }}
            }}
        '''

        # SP officer B cannot update individual for district A
        response = self.query(query_str)
        response = self.query(
            query_str,
            headers={"HTTP_AUTHORIZATION": f"Bearer {self.dist_b_user_token}"}
        )
        self.assertResponseNoErrors(response)

        content = json.loads(response.content)
        id = content['data']['updateIndividual']['internalId']
        self.assert_mutation_error(id, 'mutation.authentication_required')

        # SP officer A can update individual for district A
        response = self.query(
            query_str,
            headers={"HTTP_AUTHORIZATION": f"Bearer {self.dist_a_user_token}"}
        )
        content = json.loads(response.content)
        id = content['data']['updateIndividual']['internalId']
        self.assert_mutation_success(id)

        # SP officer B can update individual without any district
        individual_no_loc = create_individual(self.admin_user.username)
        response = self.query(
            query_str.replace(
                f'id: "{individual_a.id}"',
                f'id: "{individual_no_loc.id}"'
            ), headers={"HTTP_AUTHORIZATION": f"Bearer {self.dist_b_user_token}"}
        )
        content = json.loads(response.content)
        id = content['data']['updateIndividual']['internalId']
        self.assert_mutation_success(id)

    def test_delete_individual_general_permission(self):
        individual1 = create_individual(self.admin_user.username)
        individual2 = create_individual(self.admin_user.username)
        query_str = f'''
            mutation {{
              deleteIndividual(
                input: {{
                  ids: ["{individual1.id}", "{individual2.id}"]
                }}
              ) {{
                clientMutationId
                internalId
              }}
            }}
        '''

        # Anonymous User has no permission
        response = self.query(query_str)

        content = json.loads(response.content)
        id = content['data']['deleteIndividual']['internalId']
        self.assert_mutation_error(id, 'mutation.authentication_required')

        # Health Enrollment Officier (role=1) has no permission
        response = self.query(
            query_str,
            headers={"HTTP_AUTHORIZATION": f"Bearer {self.med_enroll_officer_token}"}
        )
        content = json.loads(response.content)
        id = content['data']['deleteIndividual']['internalId']
        self.assert_mutation_error(id, 'mutation.authentication_required')

        # IMIS admin can do everything
        response = self.query(
            query_str,
            headers={"HTTP_AUTHORIZATION": f"Bearer {self.admin_token}"}
        )
        content = json.loads(response.content)
        id = content['data']['deleteIndividual']['internalId']
        self.assert_mutation_success(id)

    def test_delete_individual_row_security(self):
        individual_a1 = create_individual(
            self.admin_user.username,
            payload_override={'village': self.village_a},
        )
        individual_a2 = create_individual(
            self.admin_user.username,
            payload_override={'village': self.village_a},
        )
        individual_b = create_individual(
            self.admin_user.username,
            payload_override={'village': self.village_b},
        )
        query_str = f'''
            mutation {{
              deleteIndividual(
                input: {{
                  ids: ["{individual_a1.id}"]
                }}
              ) {{
                clientMutationId
                internalId
              }}
            }}
        '''

        # SP officer B cannot delete individual for district A
        response = self.query(query_str)
        response = self.query(
            query_str,
            headers={"HTTP_AUTHORIZATION": f"Bearer {self.dist_b_user_token}"}
        )
        self.assertResponseNoErrors(response)

        content = json.loads(response.content)
        id = content['data']['deleteIndividual']['internalId']
        self.assert_mutation_error(id, 'mutation.authentication_required')

        # SP officer A can delete individual for district A
        response = self.query(
            query_str,
            headers={"HTTP_AUTHORIZATION": f"Bearer {self.dist_a_user_token}"}
        )
        content = json.loads(response.content)
        id = content['data']['deleteIndividual']['internalId']
        self.assert_mutation_success(id)

        # SP officer B can delete individual without any district
        individual_no_loc = create_individual(self.admin_user.username)
        response = self.query(
            query_str.replace(
                str(individual_a1.id),
                str(individual_no_loc.id)
            ), headers={"HTTP_AUTHORIZATION": f"Bearer {self.dist_b_user_token}"}
        )
        content = json.loads(response.content)
        id = content['data']['deleteIndividual']['internalId']
        self.assert_mutation_success(id)

        # SP officer B cannot delete a mix of individuals from district A and district B
        response = self.query(
            query_str.replace(
                f'["{individual_a1.id}"]',
                f'["{individual_a1.id}", "{individual_b.id}"]'
            ), headers={"HTTP_AUTHORIZATION": f"Bearer {self.dist_b_user_token}"}
        )
        content = json.loads(response.content)
        id = content['data']['deleteIndividual']['internalId']
        self.assert_mutation_error(id, 'mutation.authentication_required')

        # SP officer B can delete individual from district B
        individual_no_loc = create_individual(self.admin_user.username)
        response = self.query(
            query_str.replace(
                str(individual_a1.id),
                str(individual_b.id)
            ), headers={"HTTP_AUTHORIZATION": f"Bearer {self.dist_b_user_token}"}
        )
        content = json.loads(response.content)
        id = content['data']['deleteIndividual']['internalId']
        self.assert_mutation_success(id)
