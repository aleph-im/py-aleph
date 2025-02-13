import abc
from typing import List, Set

from aleph.db.models import MessageDb
from aleph.db.models.account_costs import AccountCostsDb
from aleph.permissions import check_sender_authorization
from aleph.types.db_session import DbSession


class ContentHandler(abc.ABC):
    async def fetch_related_content(
        self, session: DbSession, message: MessageDb
    ) -> None:
        """
        Fetch additional content from the network based on the content of a message.

        The implementation is expected to be stateless in terms of DB operations.
        Other operations like storing a file on disk are allowed.

        Note: this function should only be overridden if the content field of
        a message can contain additional data to fetch. Most message types should
        keep the default implementation.
        """
        pass

    async def is_related_content_fetched(
        self, session: DbSession, message: MessageDb
    ) -> bool:
        """
        Check whether the additional network content mentioned in the message
        is already present on the node.

        :param session: DB session.
        :param message: Message being processed.
        :return: True if all files required to process the message are fetched, False otherwise.
        """

        return True

    @abc.abstractmethod
    async def process(self, session: DbSession, messages: List[MessageDb]) -> None:
        """
        Process several messages of the same type and applies the resulting changes.

        This function is in charge of:
        * checking permissions
        * applying DB updates.
        """
        pass

    async def check_balance(
        self, session: DbSession, message: MessageDb
    ) -> List[AccountCostsDb] | None:
        """
        Checks whether the user has enough Aleph tokens to process the message.

        Raises InsufficientBalanceException if the balance of the user is too low.

        :param session: DB session.
        :param message: Message being processed.
        :return: A list of costs related with the message and the resources that are allocated
        """
        pass

    async def check_dependencies(self, session: DbSession, message: MessageDb) -> None:
        """
        Check dependencies of a message.

        Messages can depend on the prior processing of other messages on the node
        (ex: amends on a post). The implementation of this function should check for
        the presence of such objects and raise a MessageProcessingException
        (a xNotFound, usually) if the objects are not found.

        :param session: DB session.
        :param message: Message being processed.
        """
        pass

    async def check_permissions(self, session: DbSession, message: MessageDb) -> None:
        """
        Check user permissions.

        Will raise a MessageProcessingException (PermissionDenied, usually) if
        the message is not authorized to perform the requested operation.

        :param session: DB session.
        :param message: Message being processed.
        :return:
        """

        await check_sender_authorization(session=session, message=message)

    @abc.abstractmethod
    async def forget_message(self, session: DbSession, message: MessageDb) -> Set[str]:
        """
        Clean up message-type specific objects when forgetting a message.

        This operation is supposed to clean up all the objects related to a specific
        message in the DB. This may also include objects belonging to other messages
        as they are made irrelevant by the forget request (ex: amends of a post).
        In such a case, the implementation is supposed to clean up for all related
        messages and return a set of all additional messages to forget.
        This way, the forget handler can call this function once and then mark
        all related messages as forgotten.

        :param session: DB session.
        :param message: The message to forget.
        :return: The set of additional item hashes to forget.
        """
        pass
